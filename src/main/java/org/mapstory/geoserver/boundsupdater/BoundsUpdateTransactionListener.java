package org.mapstory.geoserver.boundsupdater;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

import org.eclipse.emf.ecore.EObject;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerGroupInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.util.CloseableIterator;
import org.geoserver.wfs.TransactionEvent;
import org.geoserver.wfs.TransactionEventType;
import org.geoserver.wfs.TransactionPlugin;
import org.geoserver.wfs.WFSException;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.NameImpl;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.geometry.jts.ReferencedEnvelope3D;
import org.geotools.referencing.CRS;
import org.geotools.util.logging.Logging;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.MultiValuedFilter.MatchAction;
import org.opengis.filter.Or;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import net.opengis.wfs.InsertElementType;
import net.opengis.wfs.TransactionResponseType;
import net.opengis.wfs.TransactionType;
import net.opengis.wfs.UpdateElementType;

public class BoundsUpdateTransactionListener implements TransactionPlugin {
    private static Logger log = Logging.getLogger(BoundsUpdateTransactionListener.class);
    
    static final String FEATURE_TYPE_AFFECTED_MAP = "BOUNDS_UPDATE_TRANSACTION_FEATURE_TYPE_AFFECTED_MAP";

    Catalog catalog;
    
    public BoundsUpdateTransactionListener(Catalog catalog) {
        super();
        this.catalog = catalog;
    }

    @Override
    public void dataStoreChange(TransactionEvent event) throws WFSException {
        log.info("DataStoreChange: " + event.getLayerName() + " " + event.getType());
        try {
            dataStoreChangeInternal(event);
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "Error pre computing the transaction's affected area", e);
        }
    }
    
    @Override
    public TransactionType beforeTransaction(TransactionType request)
            throws WFSException {
        return request;
    }
    
    @Override
    public void beforeCommit(TransactionType request) throws WFSException {
        // Do Nothing
    
    }
    
    @Override
    public void afterTransaction(TransactionType request,
            TransactionResponseType result, boolean committed) {
        if (!committed) {
            return;
        }
        try {
            afterTransactionInternal(request, committed);
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "Error trying to update bounds to include affected area", e);
        }
    
    }
    
    @Override
    public int getPriority() {
        return 0;
    }
    
    private void dataStoreChangeInternal(final TransactionEvent event) {
        final Object source = event.getSource();
        if (!(source instanceof InsertElementType || source instanceof UpdateElementType)) {
            // We only care about operations that potentially the bounds.
            return;
        }
        
        final EObject originatingTransactionRequest = (EObject) source;
        Objects.requireNonNull(originatingTransactionRequest, "No original transaction request exists");
        final TransactionEventType type = event.getType();
        if (TransactionEventType.POST_INSERT.equals(type)) {
            // no need to compute the bounds, they're the same as for PRE_INSERT
            return;
        }
        
        final Name featureTypeName = new NameImpl(event.getLayerName());
        final FeatureTypeInfo fti = catalog.getFeatureTypeByName(featureTypeName);
        if(Objects.isNull(fti)) {
            return;
        }
        
        final SimpleFeatureCollection affectedFeatures = event.getAffectedFeatures();
        final ReferencedEnvelope affectedBounds = affectedFeatures.getBounds();
        
        final TransactionType transaction = event.getRequest();
        
        addDirtyRegion(transaction, featureTypeName, affectedBounds);
    }
    
    @SuppressWarnings("unchecked")
    private Map<Name, Collection<ReferencedEnvelope>> getByLayerDirtyRegions(
            final TransactionType transaction) {
        
        final Map<Object, Object> extendedProperties = transaction.getExtendedProperties();
        Map<Name, Collection<ReferencedEnvelope>> byLayerDirtyRegions;
        byLayerDirtyRegions = (Map<Name, Collection<ReferencedEnvelope>>) extendedProperties
                .get(FEATURE_TYPE_AFFECTED_MAP);
        if (byLayerDirtyRegions == null) {
            byLayerDirtyRegions = new HashMap<Name, Collection<ReferencedEnvelope>>();
            extendedProperties.put(FEATURE_TYPE_AFFECTED_MAP, byLayerDirtyRegions);
        }
        return byLayerDirtyRegions;
    }
    
    private void addDirtyRegion(final TransactionType transaction, final Name featureTypeName,
            final ReferencedEnvelope affectedBounds) {
        
        Map<Name, Collection<ReferencedEnvelope>> byLayerDirtyRegions = getByLayerDirtyRegions(transaction);
        
        Collection<ReferencedEnvelope> layerDirtyRegion = byLayerDirtyRegions.get(featureTypeName);
        if (layerDirtyRegion == null) {
            layerDirtyRegion = new ArrayList<ReferencedEnvelope>(2);
            byLayerDirtyRegions.put(featureTypeName, layerDirtyRegion);
        }
        layerDirtyRegion.add(affectedBounds);
    }
    
    void updateFeatureType(FeatureTypeInfo fti, ReferencedEnvelope dirtyRegion) {
        log.fine(()->"Updating bounds of "+fti.prefixedName()+" in response to data change");
        ReferencedEnvelope bounds = fti.getNativeBoundingBox();
        bounds.expandToInclude(dirtyRegion); // CRSes should already match
        fti.setNativeBoundingBox(bounds);
        catalog.save(fti);
    }
    
    void updateLayerGroup(LayerGroupInfo lgi, ReferencedEnvelope dirtyRegion) {
        log.fine(()->"Updating bounds of "+lgi.prefixedName()+" in response to data change");
        ReferencedEnvelope bounds = lgi.getBounds();
        try {
            bounds.expandToInclude(new ReferencedEnvelope(dirtyRegion).transform(bounds.getCoordinateReferenceSystem(), true, 1000));
        } catch (MismatchedDimensionException | TransformException | FactoryException ex) {
            log.log(Level.WARNING, "Error while transforming changes to coordinate system of layer group "+lgi.prefixedName(), ex);
        }
        lgi.setBounds(bounds);
        catalog.save(lgi);
    }
    
    private void afterTransactionInternal(final TransactionType transaction, boolean committed) {
        log.fine("Detected change to data, updating bounds of affected featuer types and layer groups");
        
        final Map<Name, Collection<ReferencedEnvelope>> byLayerDirtyRegions = getByLayerDirtyRegions(transaction);
        if (byLayerDirtyRegions.isEmpty()) {
            return;
        }
        byLayerDirtyRegions.entrySet().stream().forEach(e->{
            FeatureTypeInfo fti = catalog.getFeatureTypeByName(e.getKey());
            try{
                merge(fti.getNativeBoundingBox(), e.getValue()).ifPresent(dirtyRegion->{
                    // Update the feature type
                    updateFeatureType(fti, dirtyRegion);
                    // Update all the layer groups that use it, directly or indirectly
                    StreamSupport.stream(getLayerGroupsFor(fti).spliterator(), false)
                        .forEach(lgi->updateLayerGroup(lgi, dirtyRegion));
                });
            } catch (Exception ex) {
                log.log(Level.WARNING, ex.getMessage(), ex);
                return;
            }
        });
 
    }
    
    private Optional<ReferencedEnvelope> merge(final ReferencedEnvelope oldEnv,
            final Collection<ReferencedEnvelope> dirtyList) {
        final CoordinateReferenceSystem declaredCrs = oldEnv.getCoordinateReferenceSystem();
        return dirtyList.stream()
            .map(env->{
                if(env instanceof ReferencedEnvelope3D) {
                    return new ReferencedEnvelope(env, CRS.getHorizontalCRS(env.getCoordinateReferenceSystem()));
                } else  {
                    return env;
                }
            })
            .map(env->{
                try {
                    return env.transform(declaredCrs, true, 1000);
                } catch (TransformException | FactoryException e) {
                    throw new RuntimeException("Error while merging bounding boxes",e);
                }
            })
            .reduce((env1, env2)->{ReferencedEnvelope x = new ReferencedEnvelope(env1); x.expandToInclude(env2); return x;});
    }
    
    private FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    
    // Copied this from the GWC mediator
    private Iterable<LayerGroupInfo> getLayerGroupsFor(final FeatureTypeInfo featureType) {
        List<LayerGroupInfo> layerGroups = new ArrayList<LayerGroupInfo>();
        
        // get the layers whose default style is that style, they might be in layer groups
        // using their default style
        Iterable<LayerInfo> layers = catalog.getLayers(featureType);

        // build a query retrieving the first list of candidates
        List<Filter> filters = new ArrayList<>();
        for (LayerInfo layer : layers) {
            filters.add(ff.equal(ff.property("layers.id"), ff.literal(layer.getId()), true, MatchAction.ANY));
            filters.add(ff.equal(ff.property("rootLayer.id"), ff.literal(layer.getId()), true));
        }
        Or groupFilter = ff.or(filters);
        
        try(CloseableIterator<LayerGroupInfo> it = catalog.list(LayerGroupInfo.class, groupFilter)) {
            while(it.hasNext()) {
                LayerGroupInfo lg = it.next();
                layerGroups.add(lg);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to load groups associated with feature type " + featureType.prefixedName(), e);
        }
        
        loadGroupParents(layerGroups); 
        
        return layerGroups;
    }
    
    private void loadGroupParents(List<LayerGroupInfo> layerGroups) {
        boolean foundNewParents = true;
        List<LayerGroupInfo> newGroups = new ArrayList<>(layerGroups);
        while(foundNewParents && !newGroups.isEmpty()) {
            List<Filter> parentFilters = new ArrayList<>();
            for (LayerGroupInfo lg : newGroups) {
                parentFilters.add(ff.equal(ff.property("layers.id"), ff.literal(lg.getId()), true, MatchAction.ANY));
            }
            Or parentFilter = ff.or(parentFilters);
            newGroups.clear();
            foundNewParents = false;
            try(CloseableIterator<LayerGroupInfo> it = catalog.list(LayerGroupInfo.class, parentFilter)) {
                while(it.hasNext()) {
                    LayerGroupInfo lg = it.next();
                    if(!layerGroups.contains(lg)) {
                        newGroups.add(lg);
                        layerGroups.add(lg);
                        foundNewParents = true;
                    }
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "Failed to recursively load parents group parents ");
            }
        }
    }
}
