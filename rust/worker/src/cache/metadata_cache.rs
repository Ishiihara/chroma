use crate::cache::cache::Cache;
use crate::sysdb::sysdb::SysDb;
use crate::types::Collection;
use crate::types::Segment;
use uuid::Uuid;

pub struct MetadataCache {
    collection_cache: Box<dyn Cache<Uuid, Collection>>,
    segment_cache: Box<dyn Cache<Uuid, Segment>>,
}

impl MetadataCache {
    pub fn new(
        collection_cache: Box<dyn Cache<Uuid, Collection>>,
        segment_cache: Box<dyn Cache<Uuid, Segment>>,
    ) -> Self {
        Self {
            collection_cache,
            segment_cache,
        }
    }

    pub fn insert_collection(&mut self, collection: Collection) {
        self.collection_cache.insert(collection.id, collection);
    }

    pub fn get_collection(&mut self, id: &Uuid) -> Option<Collection> {
        self.collection_cache.get(id)
    }

    pub fn insert_segment(&mut self, segment: Segment) {
        self.segment_cache.insert(segment.id, segment);
    }

    pub fn get_segment(&mut self, id: &Uuid) -> Option<Segment> {
        self.segment_cache.get(id)
    }
}

pub struct Metadata {
    sysdb: Box<dyn SysDb>,
    cache: MetadataCache,
}

impl Metadata {
    pub fn new(sysdb: Box<dyn SysDb>, cache: MetadataCache) -> Self {
        Self { sysdb, cache }
    }

    pub async fn get_collection(&mut self, id: &Uuid) -> Option<Collection> {
        match self.cache.get_collection(id) {
            Some(collection) => Some(collection),
            None => {
                let collection = self
                    .sysdb
                    .get_collections(Some(*id), None, None, None)
                    .await;
                match collection {
                    Ok(collections) => {
                        if collections.len() == 1 {
                            let collection = collections[0].clone();
                            self.cache.insert_collection(collection.clone());
                            Some(collection)
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            }
        }
    }

    pub async fn get_segment(&mut self, id: &Uuid) -> Option<Segment> {
        match self.cache.get_segment(id) {
            Some(segment) => Some(segment),
            None => {
                let segment = self.sysdb.get_segments(Some(*id), None, None, None).await;
                match segment {
                    Ok(segments) => {
                        if segments.len() == 1 {
                            let segment = segments[0].clone();
                            self.cache.insert_segment(segment.clone());
                            Some(segment)
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            }
        }
    }
}
