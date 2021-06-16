package cmd

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/hieua1/arrowcvt/inout"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

const (
	defaultPort = 8080
)

var (
	rootCmd = &cobra.Command{
		Use: "arrowcvt",
	}
	fileCmd = &cobra.Command{
		Use:   "file",
		Short: "File convert",
	}
	arrjsonCmd = &cobra.Command{
		Use:   "arrjson <arrow_file_name> <json_file_name>",
		Short: "Convert from Arrow IPC format to JSON format",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return errors.New("required exactly 2 args")
			}
			if len(args[0]) == 0 {
				return errors.New("arrow input file name must not be empty")
			}
			if len(args[1]) == 0 {
				return errors.New("json output file name must not be empty")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("Converting arrow file %s to json file %s\n...", args[0], args[1])
			fileInOut := inout.NewFileInOut(nil)
			err := fileInOut.ArrowToJSON(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Println("Done!")
			return nil
		},
	}
	jsonarrCmd = &cobra.Command{
		Use:   "jsonarr <json_file_name> <arrow_file_name>",
		Short: "Convert from JSON format to Arrow IPC format",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return errors.New("required exactly 2 args")
			}
			if len(args[0]) == 0 {
				return errors.New("json input file name must not be empty")
			}
			if len(args[1]) == 0 {
				return errors.New("arrow output file name must not be empty")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("Converting json file %s to arrow file %s\n", args[0], args[1])
			fileInOut := inout.NewFileInOut(nil)
			err := fileInOut.JSONToArrow(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Println("Done!")
			return nil
		},
	}
	serverCmd = &cobra.Command{
		Use:   "server",
		Short: "Server for serving http requests",
	}
	serverRunCmd = &cobra.Command{
		Use:   "run",
		Short: "Run http server",
		RunE: func(cmd *cobra.Command, args []string) error {
			apiInOut := inout.NewAPIInOut(nil)
			path := "/"
			if len(basePath) > 0 {
				path = path + basePath
			}
			handler := gin.Default()
			baseGroup := handler.Group(path)
			baseGroup.POST("/json-to-arrow", apiInOut.JSONToArrow)
			baseGroup.POST("/arrow-to-json", apiInOut.ArrowToJSON)
			httpServer := &http.Server{
				Addr:    fmt.Sprintf(":%v", defaultPort),
				Handler: handler,
			}
			return httpServer.ListenAndServe()
		},
	}
)

var (
	basePath = ""
)

func init() {
	rootCmd.AddCommand(fileCmd, serverCmd)
	fileCmd.AddCommand(arrjsonCmd, jsonarrCmd)
	serverCmd.AddCommand(serverRunCmd)
	serverRunCmd.Flags().StringVarP(&basePath, "base", "b", "", "Set base url path for server")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}
}
