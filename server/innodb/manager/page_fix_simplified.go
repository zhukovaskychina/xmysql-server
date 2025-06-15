package manager

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/zhukovaskychina/xmysql-server/logger"

	"github.com/zhukovaskychina/xmysql-server/server/innodb/buffer_pool"
)

// EnsurePageHasValidContent 确保页面有有效内容（简化版本）
func (idx *EnhancedBTreeIndex) EnsurePageHasValidContent(ctx context.Context, spaceID, pageNo uint32) (*buffer_pool.BufferPage, error) {
	logger.Debugf("Ensuring page %d in space %d has valid content...\n", pageNo, spaceID)

	// 1. 尝试从BufferPool获取页面
	bufferPage, err := idx.storageManager.GetBufferPoolManager().GetPage(spaceID, pageNo)
	if err != nil {
		logger.Debugf("Failed to get page from buffer pool: %v\n", err)
		return nil, err
	}

	// 2. 检查页面内容是否有效
	content := bufferPage.GetContent()
	if !idx.isValidPageContent(content) {
		logger.Debugf("Page content is invalid/empty, initializing with standard InnoDB format...\n")

		// 3. 初始化标准InnoDB页面格式
		standardContent := idx.createStandardInnoDBPage(spaceID, pageNo, idx.metadata.IndexID)
		bufferPage.SetContent(standardContent)
		bufferPage.MarkDirty()

		logger.Debugf("Page initialized with %d bytes of standard InnoDB content\n", len(standardContent))

		// 4. 立即刷新到磁盘确保持久化
		if err := idx.storageManager.GetBufferPoolManager().FlushPage(spaceID, pageNo); err != nil {
			logger.Debugf("Warning: Failed to flush initialized page: %v\n", err)
		} else {
			logger.Debugf("Initialized page flushed to disk successfully\n")
		}
	} else {
		logger.Debugf("Page already has valid content (%d bytes)\n", len(content))
	}

	return bufferPage, nil
}

// isValidPageContent 检查页面内容是否有效（简化版本）
func (idx *EnhancedBTreeIndex) isValidPageContent(content []byte) bool {
	if len(content) < 16384 {
		return false
	}

	// 检查是否全为0字节
	nonZeroFound := false
	for _, b := range content[:100] { // 检查前100字节
		if b != 0 {
			nonZeroFound = true
			break
		}
	}

	if !nonZeroFound {
		return false
	}

	// 检查InnoDB页面标识
	if len(content) >= 26 {
		// 检查页面类型是否有效 (offset 24-26)
		pageType := binary.LittleEndian.Uint16(content[24:26])
		if pageType == 0 || pageType > 50000 { // 简单的范围检查
			return false
		}
	}

	return true
}

// createStandardInnoDBPage 创建标准的InnoDB页面格式（简化版本）
func (idx *EnhancedBTreeIndex) createStandardInnoDBPage(spaceID, pageNo uint32, indexID uint64) []byte {
	pageSize := 16384 // 标准InnoDB页面大小
	pageContent := make([]byte, pageSize)

	logger.Debugf("Creating standard InnoDB page format for space %d, page %d, index %d\n", spaceID, pageNo, indexID)

	// ========================================
	// 1. 文件头 (File Header) - 38字节
	// ========================================

	// [0-4] 校验和 - 暂时设为0
	binary.LittleEndian.PutUint32(pageContent[0:4], 0)

	// [4-8] 页号
	binary.LittleEndian.PutUint32(pageContent[4:8], pageNo)

	// [8-12] 前一页号 - 暂时设为0
	binary.LittleEndian.PutUint32(pageContent[8:12], 0)

	// [12-16] 后一页号 - 暂时设为0
	binary.LittleEndian.PutUint32(pageContent[12:16], 0)

	// [16-24] LSN - 设为当前时间戳
	binary.LittleEndian.PutUint64(pageContent[16:24], uint64(time.Now().Unix()))

	// [24-26] 页面类型 - INDEX页面 (17855 = 0x45BF)
	binary.LittleEndian.PutUint16(pageContent[24:26], 17855)

	// [26-34] 文件刷新LSN
	binary.LittleEndian.PutUint64(pageContent[26:34], 0)

	// [34-38] 表空间ID
	binary.LittleEndian.PutUint32(pageContent[34:38], spaceID)

	// ========================================
	// 2. 页面头 (Page Header) - 56字节 (从偏移38开始)
	// ========================================
	pageHeaderOffset := 38

	// [38-40] 页面目录中的槽位数量 - 初始为2 (infimum + supremum)
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset:pageHeaderOffset+2], 2)

	// [40-42] 堆中记录数量 - 初始为2
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset+2:pageHeaderOffset+4], 2)

	// [42-44] 堆顶指针 - 指向第一个可用空间
	binary.LittleEndian.PutUint16(pageContent[pageHeaderOffset+4:pageHeaderOffset+6], 120) // infimum(13) + supremum(13) + 页面头(94) = 120

	// [62-70] 索引ID
	binary.LittleEndian.PutUint64(pageContent[pageHeaderOffset+24:pageHeaderOffset+32], indexID)

	// ========================================
	// 3. Infimum和Supremum记录 - 26字节 (从偏移94开始)
	// ========================================
	infimumSupremumOffset := 94

	// Infimum记录 (13字节)
	infimumOffset := infimumSupremumOffset
	// 记录头 (5字节)
	pageContent[infimumOffset] = 0x01                                               // info flags
	pageContent[infimumOffset+1] = 0x00                                             // 堆号低8位
	pageContent[infimumOffset+2] = 0x02                                             // 堆号高5位 + 记录类型(2=infimum)
	binary.LittleEndian.PutUint16(pageContent[infimumOffset+3:infimumOffset+5], 13) // 下一记录偏移
	// 记录内容 (8字节 "infimum\0")
	copy(pageContent[infimumOffset+5:infimumOffset+13], []byte("infimum\x00"))

	// Supremum记录 (13字节)
	supremumOffset := infimumOffset + 13
	// 记录头 (5字节)
	pageContent[supremumOffset] = 0x01                                               // info flags
	pageContent[supremumOffset+1] = 0x00                                             // 堆号低8位
	pageContent[supremumOffset+2] = 0x03                                             // 堆号高5位 + 记录类型(3=supremum)
	binary.LittleEndian.PutUint16(pageContent[supremumOffset+3:supremumOffset+5], 0) // 下一记录偏移 (最后一条)
	// 记录内容 (8字节 "supremum")
	copy(pageContent[supremumOffset+5:supremumOffset+13], []byte("supremum"))

	// ========================================
	// 4. 页面目录 (Page Directory) - 从页面末尾向前
	// ========================================
	directoryOffset := pageSize - 8 - 4 // 文件尾(8) + 页面目录起始

	// 页面目录槽位 - 指向infimum和supremum的位置
	binary.LittleEndian.PutUint16(pageContent[directoryOffset:directoryOffset+2], uint16(infimumOffset))
	binary.LittleEndian.PutUint16(pageContent[directoryOffset+2:directoryOffset+4], uint16(supremumOffset))

	// ========================================
	// 5. 文件尾 (File Trailer) - 8字节 (最后8字节)
	// ========================================
	trailerOffset := pageSize - 8

	// [0-4] 校验和 (与文件头一致)
	binary.LittleEndian.PutUint32(pageContent[trailerOffset:trailerOffset+4], 0)

	// [4-8] LSN低32位 (与文件头LSN的低32位一致)
	binary.LittleEndian.PutUint32(pageContent[trailerOffset+4:trailerOffset+8], uint32(time.Now().Unix()))

	logger.Debugf("Standard InnoDB page created:\n")
	logger.Debugf("   - File Header: 38 bytes\n")
	logger.Debugf("   - Page Header: 56 bytes  \n")
	logger.Debugf("   - Infimum/Supremum: 26 bytes\n")
	logger.Debugf("   - User Records: %d bytes available\n", directoryOffset-120)
	logger.Debugf("   - Page Directory: 4 bytes\n")
	logger.Debugf("   - File Trailer: 8 bytes\n")
	logger.Debugf("   - Total: %d bytes\n", pageSize)

	return pageContent
}

// VerifyPageContent 验证页面内容（简化版本）
func (idx *EnhancedBTreeIndex) VerifyPageContent(spaceID, pageNo uint32) error {
	logger.Debugf("Verifying page %d in space %d...\n", pageNo, spaceID)

	bufferPage, err := idx.storageManager.GetBufferPoolManager().GetPage(spaceID, pageNo)
	if err != nil {
		return fmt.Errorf("failed to get page: %v", err)
	}

	content := bufferPage.GetContent()
	if len(content) != 16384 {
		return fmt.Errorf("invalid page size: %d bytes (expected 16384)", len(content))
	}

	// 验证文件头
	pageNoFromHeader := binary.LittleEndian.Uint32(content[4:8])
	if pageNoFromHeader != pageNo {
		// 如果页面号为0，可能是新创建的页面，需要更新页面号
		if pageNoFromHeader == 0 {
			logger.Debugf("Page number updated successfully\n")
			// 更新页面头部的页面号
			binary.LittleEndian.PutUint32(content[4:8], pageNo)

			// 获取BufferPage并标记为脏页
			bufferPage, err := idx.storageManager.GetBufferPoolManager().GetPage(spaceID, pageNo)
			if err == nil {
				bufferPage.MarkDirty()
				// 立即刷新到磁盘
				idx.storageManager.GetBufferPoolManager().FlushPage(spaceID, pageNo)
			}

			logger.Debugf("Warning: Failed to flush updated page: %v\n", err)
		} else {
			return fmt.Errorf("page number mismatch: header says %d, expected %d", pageNoFromHeader, pageNo)
		}
	}

	spaceIDFromHeader := binary.LittleEndian.Uint32(content[34:38])
	if spaceIDFromHeader != spaceID {
		return fmt.Errorf("space ID mismatch: header says %d, expected %d", spaceIDFromHeader, spaceID)
	}

	// 验证页面类型
	pageType := binary.LittleEndian.Uint16(content[24:26])
	if pageType != 17855 { // INDEX页面类型
		logger.Debugf("Warning: Unexpected page type: %d (expected 17855 for INDEX)\n", pageType)
	}

	// 验证Infimum/Supremum
	if string(content[99:107]) != "infimum\x00" {
		return fmt.Errorf("infimum record not found at expected position")
	}

	if string(content[117:125]) != "supremum" {
		return fmt.Errorf("supremum record not found at expected position")
	}

	logger.Debugf("Page %d verification passed:\n", pageNo)
	logger.Debugf("   - Size: %d bytes\n", len(content))
	logger.Debugf("   - Page No: %d\n", pageNoFromHeader)
	logger.Debugf("   - Space ID: %d\n", spaceIDFromHeader)
	logger.Debugf("   - Page Type: %d\n", pageType)
	logger.Debugf("   - Infimum/Supremum: OK\n")

	return nil
}
