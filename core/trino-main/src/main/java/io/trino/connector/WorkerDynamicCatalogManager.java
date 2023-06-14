/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.server.ForStartup;
import io.trino.spi.connector.CatalogHandle;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.util.Executors.executeUntilFailure;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class WorkerDynamicCatalogManager
        implements ConnectorServicesProvider
{
    private static final Logger log = Logger.get(WorkerDynamicCatalogManager.class);

    private enum State { CREATED, INITIALIZED, STOPPED }

    private final CatalogStore catalogStore;
    private final CatalogFactory catalogFactory;
    private final Executor executor;

    private final Lock catalogsUpdateLock = new ReentrantLock();
    private final ConcurrentMap<CatalogHandle, CatalogConnector> catalogs = new ConcurrentHashMap<>();

    @GuardedBy("catalogsUpdateLock")
    private State state = State.CREATED;

    @Inject
    public WorkerDynamicCatalogManager(CatalogStore catalogStore, CatalogFactory catalogFactory, @ForStartup Executor executor)
    {
        this.catalogStore = requireNonNull(catalogStore, "catalogStore is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @PreDestroy
    public void stop()
    {
        List<CatalogConnector> catalogs;

        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }
            state = State.STOPPED;

            catalogs = ImmutableList.copyOf(this.catalogs.values());
            this.catalogs.clear();
        }
        finally {
            catalogsUpdateLock.unlock();
        }

        for (CatalogConnector connector : catalogs) {
            connector.shutdown();
        }
    }

    @Override
    public void loadInitialCatalogs()
    {
        catalogsUpdateLock.lock();
        try {
            if (state == State.INITIALIZED) {
                return;
            }
            checkState(state != State.STOPPED, "Worker Node is stopped");
            state = State.INITIALIZED;

            executeUntilFailure(
                    executor,
                    catalogStore.getCatalogs().stream()
                            .map(storedCatalog -> (Callable<?>) () -> {
                                CatalogProperties catalog = null;
                                try {
                                    catalog = storedCatalog.loadProperties();
                                    CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                                    catalogs.put(catalog.getCatalogHandle(), newCatalog);
                                    log.info("-- Added catalog %s using connector %s --", storedCatalog.getName(), catalog.getConnectorName());
                                }
                                catch (Throwable e) {
                                    ConnectorName connectorName = catalog != null ? catalog.getConnectorName() : new ConnectorName("unknown");
                                    log.error(e, "-- Failed to load catalog %s using connector %s --", storedCatalog.getName(), connectorName);
                                }
                                return null;
                            })
                            .collect(toImmutableList()));
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    @Override
    public void ensureCatalogsLoaded(Session session, List<CatalogProperties> expectedCatalogs)
    {
        log.info("custom: WorkerDynamicCatalogManager ensureCatalogsLoaded : " + expectedCatalogs);

        List<CatalogProperties> missingCatalogs = getMissingCatalogs(expectedCatalogs);
        if (missingCatalogs.isEmpty()) {
            return;
        }

        updateMissingCatalogs(missingCatalogs);
    }

    @Override
    public void syncCatalogs(List<CatalogProperties> catalogsInCoordinator)
    {
        log.info("custom: WorkerDynamicCatalogManager syncCatalogs : " + catalogsInCoordinator);

        List<CatalogProperties> missingCatalogs = getMissingCatalogs(catalogsInCoordinator);
        if (missingCatalogs.isEmpty()) {
            return;
        }

        updateMissingCatalogs(missingCatalogs);
    }

    private void updateMissingCatalogs(List<CatalogProperties> missingCatalogs)
    {
        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }

            for (CatalogProperties catalog : missingCatalogs) {
                checkArgument(!catalog.getCatalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE), "Global system catalog not registered");
                CatalogConnector newCatalog = catalogFactory.createCatalog(catalog);
                catalogs.put(catalog.getCatalogHandle(), newCatalog);
                catalogStore.addOrReplaceCatalog(catalog);
                log.info("Added catalog: " + catalog.getCatalogHandle());
            }
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }

    @Override
    public void pruneCatalogs(Set<CatalogHandle> catalogsInUse)
    {
        List<CatalogConnector> removedCatalogs = new ArrayList<>();
        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }

            Iterator<Entry<CatalogHandle, CatalogConnector>> iterator = catalogs.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<CatalogHandle, CatalogConnector> entry = iterator.next();
                if (!catalogsInUse.contains(entry.getKey())) {
                    iterator.remove();
                    removedCatalogs.add(entry.getValue());
                }
            }
        }
        finally {
            catalogsUpdateLock.unlock();
        }

        // todo do this in a background thread
        for (CatalogConnector removedCatalog : removedCatalogs) {
            try {
                removedCatalog.shutdown();
                catalogStore.removeCatalog(removedCatalog.getCatalog().getCatalogName());
            }
            catch (Throwable e) {
                log.error(e, "Error shutting down catalog: %s".formatted(removedCatalog));
            }
        }
        if (!removedCatalogs.isEmpty()) {
            List<String> sortedHandles = removedCatalogs.stream().map(connector -> connector.getCatalogHandle().toString()).sorted().toList();
            log.info("Pruned catalogs: %s", sortedHandles);
        }
    }

    private List<CatalogProperties> getMissingCatalogs(List<CatalogProperties> expectedCatalogs)
    {
        return expectedCatalogs.stream()
                .filter(catalog -> !catalogs.containsKey(catalog.getCatalogHandle()))
                .collect(toImmutableList());
    }

    @Override
    public ConnectorServices getConnectorServices(CatalogHandle catalogHandle)
    {
        CatalogConnector catalogConnector = catalogs.get(catalogHandle.getRootCatalogHandle());
        checkArgument(catalogConnector != null, "No catalog '%s'", catalogHandle.getCatalogName());
        return catalogConnector.getMaterializedConnector(catalogHandle.getType());
    }

    public void registerGlobalSystemConnector(GlobalSystemConnector connector)
    {
        requireNonNull(connector, "connector is null");

        catalogsUpdateLock.lock();
        try {
            if (state == State.STOPPED) {
                return;
            }

            CatalogConnector catalog = catalogFactory.createCatalog(GlobalSystemConnector.CATALOG_HANDLE, new ConnectorName(GlobalSystemConnector.NAME), connector);
            if (catalogs.putIfAbsent(GlobalSystemConnector.CATALOG_HANDLE, catalog) != null) {
                throw new IllegalStateException("Global system catalog already registered");
            }
        }
        finally {
            catalogsUpdateLock.unlock();
        }
    }
}
