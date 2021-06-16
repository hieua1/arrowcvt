package usecase

import (
	"io"

	"github.com/hieua1/arrowcvt/pkg/arrjson"

	"github.com/apache/arrow/go/arrow/arrio"
	"github.com/apache/arrow/go/arrow/ipc"
)

type Converter interface {
	ArrowToJSON(arrowSrc io.Reader, jsonDest io.Writer) error
	JSONToArrow(jsonSrc io.Reader, arrowDest io.Writer) error
}

type DefaultConverterImpl struct {
}

func NewDefaultConverterImpl() *DefaultConverterImpl {
	return &DefaultConverterImpl{}
}

func (c *DefaultConverterImpl) ArrowToJSON(arrowSrc io.Reader, jsonDest io.Writer) error {
	rr, err := ipc.NewReader(arrowSrc)
	if err != nil {
		return err
	}

	ww, err := arrjson.NewWriter(jsonDest, rr.Schema())
	if err != nil {
		return err
	}
	defer ww.Close()

	_, err = arrio.Copy(ww, rr)
	if err != nil {
		return err
	}

	return nil
}

func (c *DefaultConverterImpl) JSONToArrow(jsonSrc io.Reader, arrowDest io.Writer) error {
	rr, err := arrjson.NewReader(jsonSrc)
	if err != nil {
		return err
	}

	ww := ipc.NewWriter(arrowDest, ipc.WithSchema(rr.Schema()))
	if err != nil {
		return err
	}
	defer ww.Close()

	_, err = arrio.Copy(ww, rr)
	if err != nil {
		return err
	}

	return nil
}
