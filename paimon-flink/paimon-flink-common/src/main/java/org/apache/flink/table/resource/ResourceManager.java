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

package org.apache.flink.table.resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava31.com.google.common.io.Files;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.JarUtils;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Internal
public class ResourceManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceManager.class);
    private static final String JAR_SUFFIX = "jar";
    private static final String FILE_SCHEME = "file";
    protected final Path localResourceDir;
    private final Map<ResourceUri, ResourceCounter> functionResourceInfos;
    private final boolean cleanLocalResource;
    protected final Map<ResourceUri, URL> resourceInfos;
    protected final MutableURLClassLoader userClassLoader;
    private boolean containPython;

    public static ResourceManager createResourceManager(
            URL[] urls, ClassLoader parent, ReadableConfig config) {
        MutableURLClassLoader mutableURLClassLoader =
                FlinkUserCodeClassLoaders.create(urls, parent, config);
        return new ResourceManager(config, mutableURLClassLoader);
    }

    public ResourceManager(ReadableConfig config, MutableURLClassLoader userClassLoader) {
        this(
                new Path(
                        (String) config.get(TableConfigOptions.RESOURCES_DOWNLOAD_DIR),
                        String.format("flink-table-%s", UUID.randomUUID())),
                new HashMap(),
                new HashMap(),
                userClassLoader,
                true);
    }

    private ResourceManager(
            Path localResourceDir,
            Map<ResourceUri, URL> resourceInfos,
            Map<ResourceUri, ResourceCounter> functionResourceInfos,
            MutableURLClassLoader userClassLoader,
            boolean cleanLocalResource) {
        this.containPython = false;
        this.localResourceDir = localResourceDir;
        this.functionResourceInfos = functionResourceInfos;
        this.resourceInfos = resourceInfos;
        this.userClassLoader = userClassLoader;
        this.cleanLocalResource = cleanLocalResource;
    }

    public void registerJarResources(List<ResourceUri> resourceUris) throws IOException {
        Set<ResourceUri> jarResources =
                resourceUris.stream()
                        .filter(r -> r.getResourceType() == ResourceType.JAR)
                        .collect(Collectors.toSet());
        Set<ResourceUri> fileResources =
                resourceUris.stream()
                        .filter(r -> r.getResourceType() == ResourceType.FILE)
                        .collect(Collectors.toSet());
        this.registerResources(
                this.prepareStagingResources(
                        jarResources,
                        ResourceType.JAR,
                        true,
                        (url) -> {
                            try {
                                JarUtils.checkJarFile(url);
                            } catch (IOException e) {
                                throw new ValidationException(
                                        String.format("Failed to register jar resource [%s]", url),
                                        e);
                            }
                        },
                        false),
                true);
        this.registerResources(
                this.prepareStagingResources(
                        fileResources, ResourceType.FILE, true, (url) -> {}, false),
                true);
    }

    public String registerFileResource(ResourceUri resourceUri) throws IOException {
        Map<ResourceUri, URL> stagingResources =
                this.prepareStagingResources(
                        Collections.singletonList(resourceUri),
                        ResourceType.FILE,
                        false,
                        (url) -> {},
                        false);
        this.registerResources(stagingResources, false);
        return ((URL) this.resourceInfos.get((new ArrayList(stagingResources.keySet())).get(0)))
                .getPath();
    }

    public void declareFunctionResources(Set<ResourceUri> resourceUris) throws IOException {
        Set<ResourceUri> jarResources =
                resourceUris.stream()
                        .filter(r -> r.getResourceType() == ResourceType.JAR)
                        .collect(Collectors.toSet());
        Set<ResourceUri> fileResources =
                resourceUris.stream()
                        .filter(r -> r.getResourceType() == ResourceType.FILE)
                        .collect(Collectors.toSet());
        this.prepareStagingResources(
                jarResources,
                ResourceType.JAR,
                true,
                (url) -> {
                    try {
                        JarUtils.checkJarFile(url);
                    } catch (IOException e) {
                        throw new ValidationException(
                                String.format("Failed to register jar resource [%s]", url), e);
                    }
                },
                true);
        this.prepareStagingResources(fileResources, ResourceType.FILE, true, url -> {}, true);
    }

    public void unregisterFunctionResources(List<ResourceUri> resourceUris) {
        if (!resourceUris.isEmpty()) {
            resourceUris.forEach(
                    (uri) -> {
                        ResourceCounter counter =
                                (ResourceCounter) this.functionResourceInfos.get(uri);
                        if (counter != null && counter.decreaseCounter()) {
                            this.functionResourceInfos.remove(uri);
                        }
                    });
        }
    }

    public void registerPythonResources() {
        if (!this.containPython) {
            this.registerResources(discoverPythonDependencies(), true);
            this.containPython = true;
        }
    }

    public URLClassLoader getUserClassLoader() {
        return this.userClassLoader;
    }

    public URLClassLoader createUserClassLoader(List<ResourceUri> resourceUris) {
        if (resourceUris.isEmpty()) {
            return this.userClassLoader;
        } else {
            MutableURLClassLoader classLoader = this.userClassLoader.copy();

            for (ResourceUri resourceUri : resourceUris) {
                classLoader.addURL(
                        ((ResourceCounter)
                                        Preconditions.checkNotNull(
                                                this.functionResourceInfos.get(resourceUri)))
                                .url);
            }

            return classLoader;
        }
    }

    public Map<ResourceUri, URL> getResources() {
        return Collections.unmodifiableMap(this.resourceInfos);
    }

    public Set<URL> getLocalJarResources() {
        return (Set)
                this.resourceInfos.entrySet().stream()
                        .filter(
                                (entry) ->
                                        ResourceType.JAR.equals(
                                                ((ResourceUri) entry.getKey()).getResourceType()))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toSet());
    }

    public void addJarConfiguration(TableConfig tableConfig) {
        List<String> jars =
                (List)
                        this.getLocalJarResources().stream()
                                .map(URL::toString)
                                .collect(Collectors.toList());
        if (!jars.isEmpty()) {
            Set<String> jarFiles =
                    (Set)
                            tableConfig
                                    .getOptional(PipelineOptions.JARS)
                                    .map(LinkedHashSet::new)
                                    .orElseGet(LinkedHashSet::new);
            jarFiles.addAll(jars);
            tableConfig.set(PipelineOptions.JARS, new ArrayList(jarFiles));
        }
    }

    public ResourceManager copy() {
        return new ResourceManager(
                this.localResourceDir,
                new HashMap(this.resourceInfos),
                new HashMap(this.functionResourceInfos),
                this.userClassLoader.copy(),
                false);
    }

    public void close() throws IOException {
        this.resourceInfos.clear();
        this.functionResourceInfos.clear();
        IOException exception = null;

        try {
            this.userClassLoader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing user classloader.", e);
            exception = e;
        }

        if (this.cleanLocalResource) {
            FileSystem fileSystem = FileSystem.getLocalFileSystem();

            try {
                if (fileSystem.exists(this.localResourceDir)) {
                    fileSystem.delete(this.localResourceDir, true);
                }
            } catch (IOException ioe) {
                LOG.debug(
                        String.format("Error while delete directory [%s].", this.localResourceDir),
                        ioe);
                exception = (IOException) ExceptionUtils.firstOrSuppressed(ioe, exception);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    public boolean exists(Path filePath) throws IOException {
        return filePath.getFileSystem().exists(filePath);
    }

    public void syncFileResource(ResourceUri resourceUri, Consumer<String> resourceGenerator)
            throws IOException {
        Path targetPath = new Path(resourceUri.getUri());
        boolean remote = this.isRemotePath(targetPath);
        String localPath;
        if (remote) {
            localPath = this.getResourceLocalPath(targetPath).getPath();
        } else {
            localPath = this.getURLFromPath(targetPath).getPath();
        }

        resourceGenerator.accept(localPath);
        if (remote) {
            if (this.exists(targetPath)) {
                targetPath.getFileSystem().delete(targetPath, false);
            }

            FileUtils.copy(new Path(localPath), targetPath, false);
        }
    }

    protected void checkPath(Path path, ResourceType expectedType) throws IOException {
        FileSystem fs = FileSystem.getUnguardedFileSystem(path.toUri());
        if (!fs.exists(path)) {
            throw new FileNotFoundException(
                    String.format(
                            "%s resource [%s] not found.",
                            expectedType.name().toLowerCase(), path));
        } else if (fs.getFileStatus(path).isDir()) {
            throw new ValidationException(
                    String.format(
                            "The registering or unregistering %s resource [%s] is a directory that is not allowed.",
                            expectedType.name().toLowerCase(), path));
        } else {
            if (expectedType == ResourceType.JAR) {
                String fileExtension = Files.getFileExtension(path.getName());
                if (!fileExtension.toLowerCase().endsWith("jar")) {
                    throw new ValidationException(
                            String.format(
                                    "The registering or unregistering jar resource [%s] must ends with '.jar' suffix.",
                                    path));
                }
            }
        }
    }

    @VisibleForTesting
    URL downloadResource(Path remotePath, boolean executable) throws IOException {
        Path localPath = this.getResourceLocalPath(remotePath);

        try {
            FileUtils.copy(remotePath, localPath, executable);
            LOG.info(
                    "Download resource [{}] to local path [{}] successfully.",
                    remotePath,
                    localPath);
        } catch (IOException e) {
            throw new IOException(
                    String.format(
                            "Failed to download resource [%s] to local path [%s].",
                            remotePath, localPath),
                    e);
        }

        return this.getURLFromPath(localPath);
    }

    @VisibleForTesting
    protected URL getURLFromPath(Path path) throws IOException {
        if (path.toUri().getScheme() == null) {
            path = path.makeQualified(FileSystem.getLocalFileSystem());
        }

        return path.toUri().toURL();
    }

    @VisibleForTesting
    Path getLocalResourceDir() {
        return this.localResourceDir;
    }

    @VisibleForTesting
    boolean isRemotePath(Path path) {
        String scheme = path.toUri().getScheme();
        if (scheme == null) {
            return !"file".equalsIgnoreCase(FileSystem.getDefaultFsUri().getScheme());
        } else {
            return !"file".equalsIgnoreCase(scheme);
        }
    }

    @VisibleForTesting
    Map<ResourceUri, ResourceCounter> functionResourceInfos() {
        return this.functionResourceInfos;
    }

    private Path getResourceLocalPath(Path remotePath) {
        String fileName = remotePath.getName();
        String fileExtension = Files.getFileExtension(fileName);
        String fileNameWithUUID;
        if (StringUtils.isEmpty(fileExtension)) {
            fileNameWithUUID = String.format("%s-%s", fileName, UUID.randomUUID());
        } else {
            fileNameWithUUID =
                    String.format(
                            "%s-%s.%s",
                            Files.getNameWithoutExtension(fileName),
                            UUID.randomUUID(),
                            fileExtension);
        }

        return new Path(this.localResourceDir, fileNameWithUUID);
    }

    private static Map<ResourceUri, URL> discoverPythonDependencies() {
        try {
            URL location =
                    Class.forName(
                                    "org.apache.flink.python.PythonFunctionRunner",
                                    false,
                                    Thread.currentThread().getContextClassLoader())
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation();
            if (Paths.get(location.toURI()).toFile().isFile()) {
                return Collections.singletonMap(
                        new ResourceUri(ResourceType.JAR, location.getPath()), location);
            }
        } catch (ClassNotFoundException | URISyntaxException e) {
            LOG.warn("Failed to find flink-python jar." + e);
        }

        return Collections.emptyMap();
    }

    private void checkResources(Collection<ResourceUri> resourceUris, ResourceType expectedType)
            throws IOException {
        if (resourceUris.stream()
                .anyMatch((resourceUrix) -> expectedType != resourceUrix.getResourceType())) {
            throw new ValidationException(
                    String.format(
                            "Expect the resource type to be %s, but encounter a resource %s.",
                            expectedType.name().toLowerCase(),
                            resourceUris.stream()
                                    .filter(
                                            (resourceUrix) ->
                                                    expectedType != resourceUrix.getResourceType())
                                    .findFirst()
                                    .map(
                                            (resourceUrix) ->
                                                    String.format(
                                                            "[%s] with type %s",
                                                            resourceUrix.getUri(),
                                                            resourceUrix
                                                                    .getResourceType()
                                                                    .name()
                                                                    .toLowerCase()))
                                    .get()));
        } else {
            for (ResourceUri resourceUri : resourceUris) {
                this.checkPath(new Path(resourceUri.getUri()), expectedType);
            }
        }
    }

    private Map<ResourceUri, URL> prepareStagingResources(
            Collection<ResourceUri> resourceUris,
            ResourceType expectedType,
            boolean executable,
            Consumer<URL> resourceChecker,
            boolean declareFunctionResource)
            throws IOException {
        this.checkResources(resourceUris, expectedType);
        Map<ResourceUri, URL> stagingResourceLocalURLs = new HashMap();
        boolean supportOverwrite = !executable;

        for (ResourceUri resourceUri : resourceUris) {
            if (this.resourceInfos.containsKey(resourceUri)
                    && this.resourceInfos.get(resourceUri) != null
                    && !supportOverwrite) {
                LOG.info(
                        "Resource [{}] has been registered, overwriting of registered resource is not supported in the current version, skipping.",
                        resourceUri.getUri());
            } else {
                ResourceUri localResourceUri = resourceUri;
                URL localUrl;
                if (expectedType == ResourceType.JAR
                        && this.functionResourceInfos.containsKey(resourceUri)) {
                    localUrl = ((ResourceCounter) this.functionResourceInfos.get(resourceUri)).url;
                    ((ResourceCounter)
                                    this.functionResourceInfos.computeIfAbsent(
                                            resourceUri, (key) -> new ResourceCounter(localUrl)))
                            .increaseCounter();
                } else {
                    Path path = new Path(resourceUri.getUri());
                    if (this.isRemotePath(path)) {
                        localUrl = this.downloadResource(path, executable);
                    } else {
                        localUrl = this.getURLFromPath(path);
                        localResourceUri = new ResourceUri(expectedType, localUrl.getPath());
                    }

                    resourceChecker.accept(localUrl);
                    if (declareFunctionResource) {
                        ((ResourceCounter)
                                        this.functionResourceInfos.computeIfAbsent(
                                                resourceUri,
                                                (key) -> new ResourceCounter(localUrl)))
                                .increaseCounter();
                    }
                }

                stagingResourceLocalURLs.put(localResourceUri, localUrl);
            }
        }

        return stagingResourceLocalURLs;
    }

    private void registerResources(
            Map<ResourceUri, URL> stagingResources, boolean addToClassLoader) {
        stagingResources.forEach(
                (resourceUri, url) -> {
                    if (addToClassLoader) {
                        this.userClassLoader.addURL(url);
                        LOG.info(
                                "Added {} resource [{}] to class path.",
                                resourceUri.getResourceType().name(),
                                url);
                    }

                    this.resourceInfos.put(resourceUri, url);
                    LOG.info("Register resource [{}] successfully.", resourceUri.getUri());
                });
    }

    static class ResourceCounter {
        final URL url;
        int counter;

        private ResourceCounter(URL url) {
            this.url = url;
            this.counter = 0;
        }

        private void increaseCounter() {
            ++this.counter;
        }

        private boolean decreaseCounter() {
            --this.counter;
            Preconditions.checkState(
                    this.counter >= 0,
                    String.format("Invalid reference count[%d] which must >= 0", this.counter));
            return this.counter == 0;
        }
    }
}
