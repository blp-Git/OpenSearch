/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cli.Terminal;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.OpenSearchNodeCommand;
import org.opensearch.env.Environment;
import org.opensearch.index.translog.Checkpoint;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Command to read remote shard data.
 * This tool provides functionality to read and analyze translog data
 * from shards stored in remote store.
 *
 * @opensearch.internal
 * @since 3.0.0
 */



public class ReadRemoteDataCommand extends OpenSearchNodeCommand {
    private static final Logger logger = LogManager.getLogger(ReadRemoteDataCommand.class);

    private final OptionSpec<String> indexNameOption;
    private final OptionSpec<Integer> shardIdOption;
    private final OptionSpec<String> repoPathOption;//option

    private String indexName;
    private String indexUUID;
    private String shardId;
    private String repoPath;
    private String dataPath;

    private String basePath;
    private String indexPath;
    private String shardPath;
    private String translogPath;
    private String latestTranslogFile;
    private String primaryTermPath;
    private String checkpointFile;

    public ReadRemoteDataCommand() {
        super("read-remote-shard-data");

        indexNameOption = parser.accepts("index", "Index name")
            .withRequiredArg()
            .required();
        shardIdOption = parser.accepts("shard-id", "Shard id")
            .withRequiredArg()
            .ofType(Integer.class)
            .required();
        repoPathOption = parser.accepts("path.repo", "Repository path")
            .withRequiredArg();
    }

    @Override
    protected void processNodePaths(Terminal terminal, Path[] dataPaths, int nodeLockId, OptionSet options, Environment environment)
        throws IOException {

        // Get the values from options
        this.indexName = indexNameOption.value(options);
        this.shardId = String.valueOf(shardIdOption.value(options));
        this.dataPath = environment.settings().get("path.data").replaceAll("[\\[\\]]", "").trim();
        //construct repo path
        this.repoPath = Paths.get(dataPath)
            .getParent()
            .resolve("repo")
            .toString();

        // Retrieve indexUUID from the cluster state metadata
        final ClusterState clusterState = loadTermAndClusterState(
            createPersistedClusterStateService(environment.settings(), dataPaths),
            environment
        ).v2();
        this.indexUUID = clusterState.metadata().index(indexName).getIndexUUID();

        // Print basic information
        terminal.println("==========================================================\n");
        terminal.println("Reading Remote Shard Data\n");
        terminal.println("==========================================================\n");
        terminal.println("Index: " + indexName);
        terminal.println("Shard ID: " + shardId);
        terminal.println("indexUUID: " + indexUUID);
        terminal.println("Data path : " + dataPath);
        terminal.println("Repo path : " + repoPath);


        constructPaths(terminal);
        findLatestPrimaryTermAndTranslog(terminal);
        // Read the checkpoint from the temporary file
        Checkpoint checkpoint = Checkpoint.read(Path.of(checkpointFile));
        logCheckpointMetadata(terminal, checkpoint);
    }

    private void logCheckpointMetadata(Terminal terminal, Checkpoint checkpoint) {
        String checkpointString = checkpoint.toString();
        terminal.println("\nTranslog Checkpoint Details:");
        terminal.println("==========================");

        // Remove the "Checkpoint{" prefix and "}" suffix
        String content = checkpointString.replace("Checkpoint{", "").replace("}", "");

        // Split by comma and print each field
        for (String field : content.split(",")) {
            field = field.trim();
            terminal.println(field);
        }
    }

    private void constructPaths(Terminal terminal) {
        // Construct base path
        basePath = Paths.get(repoPath)
            .resolve("rem")  // Using 'seg' as per your gradle config
            .toString();

        // Construct index path
        indexPath = Paths.get(basePath)
            .resolve(indexUUID)
            .toString();

        // Construct shard path
        shardPath = Paths.get(indexPath)
            .resolve(shardId)
            .toString();

        // Construct translog path
        translogPath = Paths.get(shardPath)
            .resolve("translog")
            .resolve("data")
            .toString();


        // Log constructed paths
        terminal.println("\nConstructed Paths:");
        terminal.println("Base Path: " + basePath);
        terminal.println("Index Path: " + indexPath);
        terminal.println("Shard Path: " + shardPath);
        terminal.println("Translog Path: " + translogPath);
    }

    private void findLatestPrimaryTermAndTranslog(Terminal terminal) {
        Path translogDir = Paths.get(translogPath);

        if (!Files.exists(translogDir)) {
            terminal.println("\nTranslog directory does not exist: " + translogPath);
            return;
        }

        try {
            // First, find the latest primary term directory
            List<Path> primaryTermDirs = Files.list(translogDir)
                .filter(Files::isDirectory)
                .sorted((p1, p2) -> {
                    // Compare primary term numbers
                    long term1 = Long.parseLong(p1.getFileName().toString());
                    long term2 = Long.parseLong(p2.getFileName().toString());
                    return Long.compare(term2, term1);  // Descending order
                })
                .collect(Collectors.toList());

            if (primaryTermDirs.isEmpty()) {
                terminal.println("\nNo primary term directories found in: " + translogPath);
                return;
            }

            // Get the latest primary term directory
            Path latestPrimaryTermDir = primaryTermDirs.get(0);
            primaryTermPath = latestPrimaryTermDir.toString();
            terminal.println("\nLatest Primary Term Directory: " + primaryTermPath);

            // Now find the latest translog file in this primary term directory
            List<Path> translogFiles = Files.list(latestPrimaryTermDir)
                .filter(p -> p.getFileName().toString().matches("translog-\\d+\\.tlog"))
                .sorted((p1, p2) -> {
                    // Compare generation numbers
                    int gen1 = extractGeneration(p1.getFileName().toString());
                    int gen2 = extractGeneration(p2.getFileName().toString());
                    return Integer.compare(gen2, gen1);  // Descending order
                })
                .collect(Collectors.toList());

            if (!translogFiles.isEmpty()) {
                latestTranslogFile = translogFiles.get(0).toString();
                terminal.println("Latest Translog File: " + latestTranslogFile);

                // Also find corresponding checkpoint file
                checkpointFile = latestTranslogFile.replace(".tlog", ".ckp");
                if (Files.exists(Paths.get(checkpointFile))) {
                    terminal.println("Corresponding Checkpoint File: " + checkpointFile);
                }
            } else {
                terminal.println("No translog files found in primary term directory: " + primaryTermPath);
            }

        } catch (IOException e) {
            terminal.println("Error finding latest translog file: " + e.getMessage());
            logger.error("Error finding latest translog file", e);
        }
    }

    private int extractGeneration(String filename) {
        try {
            // Extract generation number from filename (e.g., "translog-2.tlog" -> 2)
            return Integer.parseInt(filename.split("-")[1].split("\\.")[0]);
        } catch (Exception e) {
            return 0;
        }
    }
}




