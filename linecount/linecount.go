package linecount

import (
	"io"
	"log"
	"os"
	"sync"
)

type FileReader struct {
	File  *os.File
	mutex sync.Mutex
}

type Chunk struct {
	Buffer []byte
}

func GetCount(chunk Chunk) int {
	lineCount := 0

	for _, b := range chunk.Buffer {
		if b == '\n' {
			lineCount++
		}
	}
	return lineCount
}

func (fileReader *FileReader) ReadChunk(buffer []byte) (Chunk, error) {
	fileReader.mutex.Lock()
	defer fileReader.mutex.Unlock()

	bytes, err := fileReader.File.Read(buffer)
	if err != nil {
		return Chunk{}, err
	}

	chunk := Chunk{buffer[:bytes]}
	return chunk, nil
}

func FileReaderCounter(fileReader *FileReader, lines chan int) {
	const bufferSize = 16 * 1024
	buffer := make([]byte, bufferSize)

	var totalRecords int

	for {
		chunk, err := fileReader.ReadChunk(buffer)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}
		totalRecords += GetCount(chunk)

	}
	lines <- totalRecords
}
