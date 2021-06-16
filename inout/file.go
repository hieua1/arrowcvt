package inout

import (
	"os"

	"github.com/hieua1/arrowcvt/usecase"
)

type FileInOut struct {
	converter usecase.Converter
}

func NewFileInOut(converter usecase.Converter) *FileInOut {
	if converter == nil {
		converter = usecase.NewDefaultConverterImpl()
	}
	return &FileInOut{
		converter: converter,
	}
}

func (f *FileInOut) JSONToArrow(jsonFile string, arrowFile string) error {
	jsonSrc, err := os.Open(jsonFile)
	if err != nil {
		return err
	}
	defer jsonSrc.Close()

	arrowDest, err := os.Create(arrowFile)
	if err != nil {
		return err
	}
	defer arrowDest.Close()

	err = f.converter.JSONToArrow(jsonSrc, arrowDest)
	if err != nil {
		return err
	}
	return nil
}

func (f *FileInOut) ArrowToJSON(arrowFile string, jsonFile string) error {
	arrowSrc, err := os.Open(arrowFile)
	if err != nil {
		return err
	}
	defer arrowSrc.Close()

	jsonDest, err := os.Create(jsonFile)
	if err != nil {
		return err
	}
	defer jsonDest.Close()
	err = f.converter.ArrowToJSON(arrowSrc, jsonDest)
	if err != nil {
		return err
	}
	return nil
}
