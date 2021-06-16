package inout

import (
	"net/http"

	"github.com/hieua1/arrowcvt/usecase"

	"github.com/gin-gonic/gin"
)

type APIInOut struct {
	converter usecase.Converter
}

func NewAPIInOut(converter usecase.Converter) *APIInOut {
	if converter == nil {
		converter = usecase.NewDefaultConverterImpl()
	}
	return &APIInOut{
		converter: converter,
	}
}

func (api *APIInOut) JSONToArrow(ctx *gin.Context) {
	err := api.converter.JSONToArrow(ctx.Request.Body, ctx.Writer)
	if err != nil {
		ctx.String(http.StatusBadRequest, err.Error())
		return
	}
	ctx.Status(http.StatusOK)
}

func (api *APIInOut) ArrowToJSON(ctx *gin.Context) {
	err := api.converter.ArrowToJSON(ctx.Request.Body, ctx.Writer)
	if err != nil {
		ctx.String(http.StatusBadRequest, err.Error())
		return
	}
	ctx.Status(http.StatusOK)
}
