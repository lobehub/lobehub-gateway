package gateway

import (
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	workspaceRoot string
)

func init() {
	workspaceRoot = os.Getenv("WORKSPACE_ROOT")
	if workspaceRoot == "" {
		workspaceRoot = "/workspace"
	}
}

// safePath 确保路径不越狱到 workspace 之外
func safePath(requestPath string) (string, error) {
	clean := filepath.Clean(filepath.Join("/", requestPath))
	fullPath := filepath.Join(workspaceRoot, clean)

	// 必须落在 workspaceRoot 内
	absRoot, _ := filepath.Abs(workspaceRoot)
	absTarget, _ := filepath.Abs(fullPath)
	if !strings.HasPrefix(absTarget, absRoot) {
		return "", os.ErrPermission
	}
	return absTarget, nil
}

func (s *Server) handleFileList(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	targetPath, err := safePath(body.Path)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": "Access denied"})
		return
	}

	entries, err := os.ReadDir(targetPath)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	files := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, map[string]any{
			"name":    entry.Name(),
			"size":    info.Size(),
			"isDir":   entry.IsDir(),
			"modTime": info.ModTime().UTC().Format(time.RFC3339),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"success": true, "files": files})
}

func (s *Server) handleFileRead(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	targetPath, err := safePath(body.Path)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": "Access denied"})
		return
	}

	data, err := os.ReadFile(targetPath)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	// 判断文件大小，超过 1MB 只返回 metadata
	if len(data) > 1024*1024 {
		writeJSON(w, http.StatusOK, map[string]any{
			"success":   true,
			"size":      len(data),
			"mimeType":  mimeTypeByExt(targetPath),
			"truncated": true,
			"message":   "File too large (>1MB), use list endpoint to check metadata",
		})
		return
	}

	// 检测是否是文本文件
	mimeType := mimeTypeByExt(targetPath)
	if isTextMIME(mimeType) {
		writeJSON(w, http.StatusOK, map[string]any{
			"success":  true,
			"content":  string(data),
			"mimeType": mimeType,
			"size":     len(data),
		})
	} else {
		// 二进制文件用 base64
		writeJSON(w, http.StatusOK, map[string]any{
			"success":  true,
			"content":  base64.StdEncoding.EncodeToString(data),
			"mimeType": mimeType,
			"encoding": "base64",
			"size":     len(data),
		})
	}
}

func (s *Server) handleFileWrite(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	targetPath, err := safePath(body.Path)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": "Access denied"})
		return
	}

	// 确保父目录存在
	parentDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	if err := os.WriteFile(targetPath, []byte(body.Content), 0644); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handleFileDelete(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	targetPath, err := safePath(body.Path)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": "Access denied"})
		return
	}

	if err := os.Remove(targetPath); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handleFileMkdir(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	targetPath, err := safePath(body.Path)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": "Access denied"})
		return
	}

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handleFileMove(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	if body.Content == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "Missing destination path in content field"})
		return
	}

	srcPath, err := safePath(body.Path)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": "Access denied"})
		return
	}

	dstPath, err := safePath(body.Content)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"success": false, "error": "Access denied"})
		return
	}

	// 确保目标父目录存在
	parentDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	if err := os.Rename(srcPath, dstPath); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"success": false, "error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func mimeTypeByExt(path string) string {
	switch {
	case strings.HasSuffix(path, ".pdf"):
		return "application/pdf"
	case strings.HasSuffix(path, ".txt"), strings.HasSuffix(path, ".md"):
		return "text/plain"
	case strings.HasSuffix(path, ".json"):
		return "application/json"
	case strings.HasSuffix(path, ".csv"):
		return "text/csv"
	case strings.HasSuffix(path, ".html"), strings.HasSuffix(path, ".htm"):
		return "text/html"
	case strings.HasSuffix(path, ".xml"):
		return "application/xml"
	case strings.HasSuffix(path, ".png"):
		return "image/png"
	case strings.HasSuffix(path, ".jpg"), strings.HasSuffix(path, ".jpeg"):
		return "image/jpeg"
	case strings.HasSuffix(path, ".py"):
		return "text/x-python"
	case strings.HasSuffix(path, ".go"):
		return "text/x-go"
	case strings.HasSuffix(path, ".js"), strings.HasSuffix(path, ".mjs"):
		return "text/javascript"
	case strings.HasSuffix(path, ".ts"):
		return "text/typescript"
	case strings.HasSuffix(path, ".yaml"), strings.HasSuffix(path, ".yml"):
		return "text/yaml"
	default:
		return "application/octet-stream"
	}
}

func isTextMIME(mimeType string) bool {
	return strings.HasPrefix(mimeType, "text/") ||
		mimeType == "application/json" ||
		mimeType == "application/xml"
}
