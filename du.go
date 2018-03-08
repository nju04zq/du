package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	// 4 threads, get 3x speed
	DU_PARALLEL_ROUTINES = 4
)

type DirEntry struct {
	Name string
	Size int64
}

type duStats struct {
	maxLevel int
	size     int
	levels   []map[string]*DirEntry
}

func (stats *duStats) init(level int) {
	if level == 0 {
		return
	}
	stats.maxLevel = level
	stats.levels = make([]map[string]*DirEntry, level+1)
	for i, _ := range stats.levels {
		stats.levels[i] = make(map[string]*DirEntry)
	}
}

func (stats *duStats) pathAt(path string, level int) string {
	var i, idx int
	for i = 0; i < len(path); i++ {
		if path[i] == filepath.Separator {
			idx++
			if idx == level {
				break
			}
		}
	}
	return path[:i]
}

func (stats *duStats) pathLevel(path string) int {
	level := 1
	for i := 0; i < len(path); i++ {
		if path[i] == filepath.Separator {
			level++
		}
	}
	return level
}

func (stats *duStats) addTo(entry *DirEntry, level int) {
	lstats := stats.levels[level]
	path := stats.pathAt(entry.Name, level)
	upperEntry, ok := lstats[path]
	if !ok {
		upperEntry = &DirEntry{Name: path}
		lstats[path] = upperEntry
	}
	upperEntry.Size += entry.Size
}

func (stats *duStats) update(entry *DirEntry, level int) {
	if level <= stats.maxLevel {
		stats.levels[level][entry.Name] = entry
	} else {
		stats.addTo(entry, stats.maxLevel)
	}
}

func (stats *duStats) conclude() []*DirEntry {
	if stats.maxLevel == 0 {
		return nil
	}
	for _, entry := range stats.levels[stats.maxLevel] {
		for level := stats.maxLevel - 1; level >= 1; level-- {
			stats.addTo(entry, level)
		}
	}
	entries := make([]*DirEntry, 0)
	for _, lstats := range stats.levels {
		for _, entry := range lstats {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		ei, ej := entries[i], entries[j]
		if ei.Size > ej.Size {
			return true
		} else if ei.Size < ej.Size {
			return false
		} else {
			return stats.pathLevel(ei.Name) < stats.pathLevel(ej.Name)
		}
	})
	return entries
}

type duEntry struct {
	level int
	path  string
	next  *duEntry
}

type duCtrl struct {
	mutex    sync.Mutex
	cond     *sync.Cond
	root     string
	maxLevel int
	stats    duStats
	size     int64
	total    int
	free     int
	done     bool
	head     *duEntry
	tail     *duEntry
}

func (ctrl *duCtrl) init(root string, maxLevel int) *duCtrl {
	ctrl.cond = sync.NewCond(&ctrl.mutex)
	ctrl.total = DU_PARALLEL_ROUTINES
	ctrl.root = root
	ctrl.maxLevel = maxLevel
	ctrl.stats.init(maxLevel)
	ctrl.insert(".", 0)
	return ctrl
}

func (ctrl *duCtrl) update(size int64, entries []*DirEntry, level int) {
	ctrl.mutex.Lock()
	defer ctrl.mutex.Unlock()
	ctrl.size += size
	for _, entry := range entries {
		ctrl.stats.update(entry, level)
	}
}

func (ctrl *duCtrl) insert(path string, level int) {
	entry := &duEntry{
		path:  path,
		level: level,
	}
	if ctrl.head == nil {
		ctrl.head, ctrl.tail = entry, entry
	} else {
		ctrl.tail.next = entry
		ctrl.tail = entry
	}
}

func (ctrl *duCtrl) remove() (string, int) {
	if ctrl.head == nil {
		return "", 0
	}
	path, level := ctrl.head.path, ctrl.head.level
	ctrl.head = ctrl.head.next
	if ctrl.head == nil {
		ctrl.tail = nil
	}
	return path, level
}

func (ctrl *duCtrl) get() (string, int) {
	ctrl.mutex.Lock()
	defer ctrl.mutex.Unlock()
	ctrl.free++
	if ctrl.free == ctrl.total && ctrl.head == nil {
		ctrl.done = true
		ctrl.cond.Broadcast()
		return "", 0
	}
	for ctrl.head == nil && !ctrl.done {
		ctrl.cond.Wait()
	}
	if ctrl.done {
		return "", 0
	}
	ctrl.free--
	return ctrl.remove()
}

func (ctrl *duCtrl) put(path string, level int) {
	ctrl.mutex.Lock()
	defer ctrl.mutex.Unlock()
	ctrl.insert(path, level)
	ctrl.cond.Signal()
}

func joinPath(p0, p1 string) string {
	if p0 == "." {
		return p1
	} else {
		return filepath.Join(p0, p1)
	}
}

func duHelper(wg *sync.WaitGroup, ctrl *duCtrl) {
	var entries []*DirEntry
	defer wg.Done()
	for {
		path, level := ctrl.get()
		if path == "" {
			break
		}
		abspath := filepath.Join(ctrl.root, path)
		level++
		var size int64
		finfos, err := ioutil.ReadDir(abspath)
		if err != nil {
			continue
		}
		if ctrl.maxLevel > 0 {
			entries = make([]*DirEntry, 0, len(finfos))
		}
		for _, finfo := range finfos {
			fpath := joinPath(path, finfo.Name())
			if finfo.IsDir() {
				ctrl.put(fpath, level)
				continue
			}
			size += finfo.Size()
			if ctrl.maxLevel > 0 {
				entries = append(entries, &DirEntry{
					Name: fpath,
					Size: finfo.Size(),
				})
			}
		}
		ctrl.update(size, entries, level)
	}
}

func duLevel(path string, level int) (int64, []*DirEntry, error) {
	wg := new(sync.WaitGroup)
	ctrl := new(duCtrl).init(path, level)
	for i := 0; i < DU_PARALLEL_ROUTINES; i++ {
		wg.Add(1)
		go duHelper(wg, ctrl)
	}
	wg.Wait()
	size := ctrl.size
	entries := ctrl.stats.conclude()
	return size, entries, nil
}

func Du(path string) (int64, error) {
	log.Printf("du %s", path)
	finfo, err := os.Stat(path)
	if err != nil {
		log.Printf("Error du %s, %v", path, err)
		return 0, err
	}
	if !finfo.IsDir() {
		return finfo.Size(), nil
	}
	size, _, err := duLevel(path, 0)
	if err != nil {
		log.Printf("Fail on du %s, %v", path, err)
	} else {
		log.Printf("du %s, get size %d", path, size)
	}
	return size, err
}

func DuChilds(path string) ([]*DirEntry, error) {
	log.Printf("du %s for child entries", path)
	finfo, err := os.Stat(path)
	if err != nil {
		log.Printf("Error du %s, %v", path, err)
		return nil, err
	}
	if !finfo.IsDir() {
		return []*DirEntry{&DirEntry{finfo.Name(), finfo.Size()}}, nil
	}
	_, entries, err := duLevel(path, 1)
	if err != nil {
		log.Printf("Fail on du %s, %v", path, err)
	} else {
		log.Printf("du %s done, get %d entries", path, len(entries))
	}
	return entries, err
}
