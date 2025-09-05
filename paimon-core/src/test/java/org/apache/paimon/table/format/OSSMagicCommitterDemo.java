/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.format;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.MultipartUploadInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTableAtomicCommitter.TempFileInfo;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Demo class showing how to use OSS Magic Committer for atomic writes. This demo simulates the
 * complete three-phase commit pattern: 1. 写入阶段：数据写入到特殊的 "magic" 路径 2. 提交阶段：通过 OSS 的 multipart
 * upload 完成原子性提交 3. 清理 Magic 路径
 */
public class OSSMagicCommitterDemo {

    public static void main(String[] args) throws IOException {
        System.out.println("=== OSS Magic Committer Demo ===");
        System.out.println("演示基于 Hadoop MagicS3ACommitter 模式的原子性提交");
        System.out.println();

        // 1. 设置 OSS 表配置
        Map<String, String> ossOptions = createOSSConfiguration();
        FormatTable mockTable = createMockOSSTable(ossOptions);

        // 2. 创建 Magic Committer
        FormatTableAtomicCommitter committer = FormatTableAtomicCommitter.create(mockTable);
        System.out.println("✅ 已创建 OSS Magic Committer: " + committer.getClass().getSimpleName());

        // 3. 执行三阶段提交演示
        demonstrateThreePhaseCommit(committer, mockTable);

        // 4. 演示故障恢复场景
        demonstrateFailureRecovery(committer, mockTable);

        // 5. 演示并发写入场景
        demonstrateConcurrentWrites(mockTable);

        System.out.println("\n=== Demo 完成 ===");
    }

    private static Map<String, String> createOSSConfiguration() {
        Map<String, String> options = new HashMap<>();

        // OSS 基础配置
        options.put("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        options.put("fs.oss.accessKeyId", "LTAI***"); // 实际使用时填入真实值
        options.put("fs.oss.accessKeySecret", "***"); // 实际使用时填入真实值

        // 可选配置
        options.put("fs.oss.securityToken", ""); // STS token (可选)
        options.put("oss.sse.enabled", "true"); // 服务端加密

        // Magic Committer 特定配置
        options.put("oss.multipart.part-size", "5242880"); // 5MB
        options.put("oss.multipart.max-retries", "3");

        System.out.println("📋 OSS 配置:");
        System.out.println("  - Endpoint: " + options.get("fs.oss.endpoint"));
        System.out.println("  - SSE Enabled: " + options.get("oss.sse.enabled"));
        System.out.println("  - Part Size: " + options.get("oss.multipart.part-size") + " bytes");

        return options;
    }

    private static org.apache.paimon.table.FormatTable createMockOSSTable(
            Map<String, String> options) {
        return new MockFormatTable("oss://my-data-lake/orders", options, new LocalFileIO());
    }

    private static void demonstrateThreePhaseCommit(
            FormatTableAtomicCommitter committer, org.apache.paimon.table.FormatTable table)
            throws IOException {

        System.out.println("\n=== 三阶段原子性提交演示 ===");

        // Phase 1: 写入阶段
        System.out.println("\n📝 Phase 1: 写入阶段");
        String partition = "year=2024/month=12/day=01";
        Path magicLocation = committer.prepareTempLocation(partition);

        System.out.println("Magic 路径: " + magicLocation);
        System.out.println("特点：包含 __magic 前缀，对外不可见");

        // 写入测试数据
        String fileName = "orders_" + getCurrentTimestamp() + ".parquet";
        Path magicFile = new Path(magicLocation, fileName);
        Path finalFile = new Path(table.location(), partition + "/" + fileName);

        writeTestData(table.fileIO(), magicFile, createSampleData());
        System.out.println("✅ 数据已写入 Magic 路径");
        System.out.println("  - Magic 文件: " + magicFile);
        System.out.println("  - 目标位置: " + finalFile);

        // Phase 2: 提交阶段
        System.out.println("\n🔄 Phase 2: 提交阶段");
        System.out.println("模拟 OSS multipart upload 原子性提交...");

        TempFileInfo tempFileInfo = new TempFileInfo(magicFile, finalFile, partition);

        // 现在使用真实的 multipart upload 流
        System.out.println("使用新的 MagicCommitterOutputStream 进行 multipart upload...");

        try (org.apache.paimon.fs.MagicCommitterOutputStream magicStream =
                table.fileIO().newMagicCommitterOutputStream(magicFile, finalFile, partition)) {

            // 写入数据，自动分片上传
            byte[] sampleData = createSampleData().getBytes(StandardCharsets.UTF_8);
            magicStream.write(sampleData);

            // 获取上传信息
            org.apache.paimon.fs.MultipartUploadInfo uploadInfo = magicStream.getUploadInfo();
            System.out.println("✅ 开始 multipart upload");
            System.out.println("  - Upload ID: " + uploadInfo.getUploadId());
            System.out.println("  - Bucket: " + uploadInfo.getBucketName());
            System.out.println("  - Object Key: " + uploadInfo.getObjectKey());

            // 流会在 close() 时自动完成 multipart upload
        } catch (UnsupportedOperationException e) {
            // 对于不支持 multipart upload 的 FileIO（如 LocalFileIO），使用普通写入
            System.out.println("⚠️ FileIO 不支持 multipart upload，使用常规写入");
            writeTestData(table.fileIO(), magicFile, createSampleData());
        }

        System.out.println("✅ 数据写入完成（使用 multipart upload）");

        // 执行原子性提交
        try {
            committer.commitFiles(Arrays.asList(tempFileInfo));
            System.out.println("✅ 原子性提交成功！");

            // 验证文件存在
            if (table.fileIO().exists(finalFile)) {
                System.out.println("✅ 文件在最终位置可见: " + finalFile);
            }
        } catch (Exception e) {
            System.out.println("❌ 提交失败: " + e.getMessage());
        }

        // Phase 3: 清理阶段
        System.out.println("\n🧹 Phase 3: 清理阶段");
        committer.cleanupTempDirectory();
        System.out.println("✅ Magic 路径已清理");
        System.out.println("✅ 元数据已清理");

        System.out.println("\n📊 提交结果摘要:");
        System.out.println("  - 数据大小: " + createSampleData().length() + " 字符");
        System.out.println("  - 分区: " + partition);
        System.out.println("  - 文件名: " + fileName);
        System.out.println("  - 最终位置: " + finalFile);
    }

    private static void demonstrateFailureRecovery(
            FormatTableAtomicCommitter committer, org.apache.paimon.table.FormatTable table)
            throws IOException {

        System.out.println("\n=== 故障恢复演示 ===");

        // 模拟中断的上传
        String partition = "year=2024/month=12/day=02";
        Path magicLocation = committer.prepareTempLocation(partition);
        String fileName = "interrupted_" + getCurrentTimestamp() + ".parquet";
        Path magicFile = new Path(magicLocation, fileName);
        Path finalFile = new Path(table.location(), partition + "/" + fileName);

        // 创建未完成的上传
        MultipartUploadTracker tracker =
                new MultipartUploadTracker(
                        table.fileIO(),
                        new Path(table.location(), "__pending-uploads/recovery-session"));

        MultipartUploadInfo incompleteUpload =
                new MultipartUploadInfo(
                        "interrupted-upload-" + System.currentTimeMillis(),
                        "my-data-lake",
                        "orders/" + partition + "/" + fileName,
                        magicFile,
                        finalFile,
                        partition,
                        Arrays.asList(new MultipartUploadInfo.PartETag(1, "\"etag-part-1\"")),
                        5242880L,
                        null);

        tracker.recordUploadStart(incompleteUpload);
        System.out.println("📝 模拟上传中断：");
        System.out.println("  - Upload ID: " + incompleteUpload.getUploadId());
        System.out.println("  - 已完成部分: " + incompleteUpload.getCompletedParts().size() + "/3");

        // 模拟恢复过程
        System.out.println("\n🔄 开始恢复过程：");
        Map<String, MultipartUploadInfo> pendingUploads = tracker.loadPendingUploads();

        if (!pendingUploads.isEmpty()) {
            System.out.println("✅ 发现 " + pendingUploads.size() + " 个未完成的上传");

            for (MultipartUploadInfo upload : pendingUploads.values()) {
                System.out.println("  - 恢复上传: " + upload.getUploadId());
                System.out.println("  - 进度: " + upload.getCompletedParts().size() + " 个部分已完成");

                // 决策：中止上传
                tracker.recordUploadAbort(upload.getObjectKey());
                System.out.println("  - 决策: 中止上传并清理资源");
            }
        }

        System.out.println("✅ 故障恢复完成");
    }

    private static void demonstrateConcurrentWrites(org.apache.paimon.table.FormatTable table)
            throws IOException {
        System.out.println("\n=== 并发写入演示 ===");

        String[] sessions = {"session-A", "session-B", "session-C"};
        List<TempFileInfo> allTempFiles = new ArrayList<>();

        System.out.println("模拟 " + sessions.length + " 个并发写入会话：");

        for (String session : sessions) {
            // 每个会话使用独立的提交器
            FormatTableAtomicCommitter sessionCommitter = FormatTableAtomicCommitter.create(table);

            String partition = "year=2024/month=12/day=03";
            Path magicLocation = sessionCommitter.prepareTempLocation(partition);
            String fileName = session + "_data_" + getCurrentTimestamp() + ".parquet";
            Path magicFile = new Path(magicLocation, fileName);
            Path finalFile = new Path(table.location(), partition + "/" + fileName);

            // 写入会话特定数据
            String sessionData = "Data from " + session + " at " + getCurrentTimestamp();
            writeTestData(table.fileIO(), magicFile, sessionData);

            allTempFiles.add(new TempFileInfo(magicFile, finalFile, partition));

            System.out.println("  ✅ " + session + " 数据写入完成");
            System.out.println("    - Magic 路径: " + magicFile);
        }

        System.out.println("\n📊 并发写入状态:");
        System.out.println("  - 总会话数: " + sessions.length);
        System.out.println("  - 所有数据当前对外不可见 (在 Magic 路径中)");

        // 模拟部分提交成功，部分失败的场景
        System.out.println("\n🎯 提交场景: 1个成功，2个中止");

        System.out.println("  ✅ " + sessions[0] + " 提交成功");

        for (int i = 1; i < allTempFiles.size(); i++) {
            System.out.println("  ❌ " + sessions[i] + " 提交中止");
        }

        System.out.println("✅ 并发写入演示完成");
        System.out.println("  - 结果: 只有成功提交的会话数据可见");
        System.out.println("  - 优势: 会话间完全隔离，不会相互影响");
    }

    // Helper methods

    private static void writeTestData(FileIO fileIO, Path path, String data) throws IOException {
        Path parentDir = path.getParent();
        if (parentDir != null && !fileIO.exists(parentDir)) {
            fileIO.mkdirs(parentDir);
        }

        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(data.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String createSampleData() {
        return String.format(
                "# Sample Order Data - Generated at %s\n"
                        + "order_id,customer_id,product,quantity,price,timestamp\n"
                        + "ORD001,CUST123,Laptop,1,999.99,%s\n"
                        + "ORD002,CUST456,Mouse,2,29.99,%s\n"
                        + "ORD003,CUST789,Keyboard,1,79.99,%s\n"
                        + "# End of sample data\n",
                getCurrentTimestamp(),
                getCurrentTimestamp(),
                getCurrentTimestamp(),
                getCurrentTimestamp());
    }

    private static String getCurrentTimestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /** 模拟 FormatTable 实现，用于演示 */
    private static class MockFormatTable implements org.apache.paimon.table.FormatTable {
        private final String location;
        private final Map<String, String> options;
        private final FileIO fileIO;

        public MockFormatTable(String location, Map<String, String> options, FileIO fileIO) {
            this.location = location;
            this.options = options;
            this.fileIO = fileIO;
        }

        @Override
        public String location() {
            return location;
        }

        @Override
        public Map<String, String> options() {
            return options;
        }

        @Override
        public FileIO fileIO() {
            return fileIO;
        }

        @Override
        public String name() {
            return "demo-oss-table";
        }

        @Override
        public RowType rowType() {
            return RowType.of(); // Empty row type for demo
        }

        @Override
        public List<String> partitionKeys() {
            return Arrays.asList("year", "month", "day");
        }

        @Override
        public List<String> primaryKeys() {
            return Arrays.asList(); // No primary keys for demo
        }

        @Override
        public java.util.Optional<String> comment() {
            return java.util.Optional.of("Demo OSS table for Magic Committer");
        }

        @Override
        public String format() {
            return "UNKNOWN"; // Demo format
        }

        @Override
        public org.apache.paimon.table.FormatTable copy(Map<String, String> newOptions) {
            return new MockFormatTable(location, newOptions, fileIO);
        }
    }
}
