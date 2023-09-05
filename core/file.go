package core

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
)

type FileInfo struct {
	fullPath string
	Name     string
	Type     string
}

func (x *FileInfo) FullPath() string {
	return x.fullPath
}

type traverser struct {
	suffix          string
	entries         []FileInfo
	extensionSet    map[string]struct{}
	isAllExtensions bool
}

func NewTraverser(extensionList []string) *traverser {
	isAllExtensions := false
	if extensionList[0] == "all" {
		isAllExtensions = true
	}
	extensionSet := make(map[string]struct{})
	for _, e := range extensionList {
		extensionSet[e] = struct{}{}
	}

	return &traverser{
		suffix:          "",
		entries:         make([]FileInfo, 0),
		extensionSet:    extensionSet,
		isAllExtensions: isAllExtensions,
	}
}

func (x *traverser) Do(root string) ([]FileInfo, error) {
	x.entries = make([]FileInfo, 0)
	/*root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	} */
	x.traverse(root)
	return x.entries, nil
}

func (x *traverser) traverse(parent string) {
	files, err := ioutil.ReadDir(parent)
	if err != nil {
		return
	}

	for _, file := range files {
		fullpath := fmt.Sprintf("%s/%s", parent, file.Name())
		if file.IsDir() {
			x.traverse(fullpath)
		} else {
			ext := filepath.Ext(fullpath)
			if len(ext) == 0 {
				continue
			} else {
				ext = strings.ToLower(filepath.Ext(fullpath)[1:])
			}
			if x.isAllExtensions {
				entry := FileInfo{
					fullPath: fullpath,
					Name:     file.Name(),
					Type:     ext,
				}
				x.entries = append(x.entries, entry)
			} else {
				if _, ok := x.extensionSet[ext]; ok {
					entry := FileInfo{
						fullPath: fullpath,
						Name:     file.Name(),
						Type:     ext,
					}
					x.entries = append(x.entries, entry)
				}
			}
		}
	}
}
