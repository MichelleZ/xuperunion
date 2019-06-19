/*
 * Copyright (c) 2019. Baidu Inc. All Rights Reserved.
 */

package log

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/xuperchain/xuperunion/common/config"
)

// CreateLog get xlog
func CreateLog() *config.LogConfig {
	logFolder, err := makeDir("logs")
	if err != nil {
		fmt.Println("make Dir failed.")
	}

	logConfig := &config.LogConfig{
		Module:         "gateway",
		Filepath:       logFolder,
		Filename:       "gateway",
		Fmt:            "logfmt",
		Console:        true,
		Level:          "trace",
		Async:          false,
		RotateInterval: 60 * 24, // every day
		RotateBackups:  7,       // save 7 days
	}

	return logConfig
}

// makeDir generate log path
func makeDir(pth string) (string, error) {
	xchainRoot := os.Getenv("XCHAIN_ROOT")
	if xchainRoot != "" {
		fmt.Println("logs path:", path.Join(xchainRoot, pth))
		return path.Join(xchainRoot, pth), nil
	}

	return filepath.Abs(pth)
}
