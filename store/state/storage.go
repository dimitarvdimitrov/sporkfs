package state

import "github.com/dimitarvdimitrov/sporkfs/store"

func readFiles(location string) *fileNode {
	return &fileNode{
		File: &store.File{
			Id:   0,
			Name: "",
			Mode: store.ModeDirectory,
			Size: 1,
		},
		children: []*fileNode{
			{
				File: &store.File{
					Id:   2,
					Name: "2.txt",
					Mode: store.ModeRegularFile,
					Size: 5,
				},
			},
			{
				File: &store.File{
					Id:   3,
					Name: "3",
					Mode: store.ModeDirectory,
					Size: 1,
				},
				children: []*fileNode{
					{
						File: &store.File{
							Id:   4,
							Name: "4.txt",
							Mode: store.ModeRegularFile,
							Size: 5,
						},
					},
					{
						File: &store.File{
							Id:   5,
							Name: "5.txt",
							Mode: store.ModeRegularFile,
							Size: 5,
						},
					},
				},
			},
		},
	}
}
