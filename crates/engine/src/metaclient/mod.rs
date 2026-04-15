use crate::error::Result;
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub is_leader: bool,
    pub term: u64,
}

impl NodeInfo {
    pub fn new(id: u64, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            is_leader: false,
            term: 0,
        }
    }

    pub fn with_leader(mut self, is_leader: bool) -> Self {
        self.is_leader = is_leader;
        self
    }

    pub fn with_term(mut self, term: u64) -> Self {
        self.term = term;
        self
    }
}

#[derive(Debug, Clone)]
pub struct ShardMapping {
    pub shard_id: u64,
    pub node_id: u64,
    pub database: String,
    pub retention_policy: String,
    pub start_time: i64,
    pub end_time: i64,
}

impl ShardMapping {
    pub fn new(
        shard_id: u64,
        node_id: u64,
        database: &str,
        rp: &str,
        start_time: i64,
        end_time: i64,
    ) -> Self {
        Self {
            shard_id,
            node_id,
            database: database.to_string(),
            retention_policy: rp.to_string(),
            start_time,
            end_time,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    pub name: String,
    pub shard_groups: Vec<ShardGroup>,
}

#[derive(Debug, Clone)]
pub struct ShardGroup {
    pub id: u64,
    pub database: String,
    pub retention_policy: String,
    pub start_time: i64,
    pub end_time: i64,
    pub shards: Vec<ReplicaShardInfo>,
}

#[derive(Debug, Clone)]
pub struct ReplicaShardInfo {
    pub id: u64,
    pub group_id: u64,
    pub node_ids: Vec<u64>,
    pub owns: bool,
}

pub trait MetaClient: Send + Sync {
    fn node_id(&self) -> u64;
    fn leader(&self) -> Option<NodeInfo>;
    fn peers(&self) -> Vec<NodeInfo>;
    fn is_leader(&self) -> bool;
    fn create_database(&mut self, name: &str) -> Result<DatabaseInfo>;
    fn get_database(&self, name: &str) -> Result<Option<DatabaseInfo>>;
    fn list_databases(&self) -> Result<Vec<String>>;
    fn create_retention_policy(&mut self, db: &str, name: &str, duration: u64, replica: u32) -> Result<()>;
    fn get_retention_policy(&self, db: &str, name: &str) -> Result<Option<crate::schema::RetentionPolicy>>;
    fn create_measurement(&mut self, db: &str, rp: &str, name: &str) -> Result<()>;
    fn update_shard_readers(&mut self, shard_mapping: &ShardMapping) -> Result<()>;
    fn get_shard_mappings(&self, db: &str, rp: &str, start: i64, end: i64) -> Result<Vec<ShardMapping>>;
    fn get_shard_owner(&self, shard_id: u64) -> Result<Option<ShardMapping>>;
    fn acquire_node(&self) -> Result<u64>;
    fn node(&self) -> Result<NodeInfo>;
}

pub struct MetaClientStub {
    node_id: u64,
    peers: Vec<NodeInfo>,
    databases: HashMap<String, DatabaseInfo>,
    shard_mappings: HashMap<u64, ShardMapping>,
    retention_policies: HashMap<String, HashMap<String, crate::schema::RetentionPolicy>>,
}

impl MetaClientStub {
    pub fn new(node_id: u64, peers: Vec<NodeInfo>) -> Self {
        Self {
            node_id,
            peers,
            databases: HashMap::new(),
            shard_mappings: HashMap::new(),
            retention_policies: HashMap::new(),
        }
    }

    pub fn with_database(mut self, db: DatabaseInfo) -> Self {
        self.databases.insert(db.name.clone(), db);
        self
    }
}

impl MetaClient for MetaClientStub {
    fn node_id(&self) -> u64 {
        self.node_id
    }

    fn leader(&self) -> Option<NodeInfo> {
        self.peers.iter().find(|p| p.is_leader).cloned()
    }

    fn peers(&self) -> Vec<NodeInfo> {
        self.peers.clone()
    }

    fn is_leader(&self) -> bool {
        self.leader().is_some()
    }

    fn create_database(&mut self, name: &str) -> Result<DatabaseInfo> {
        let db = DatabaseInfo {
            name: name.to_string(),
            shard_groups: Vec::new(),
        };
        self.databases.insert(name.to_string(), db.clone());
        Ok(db)
    }

    fn get_database(&self, name: &str) -> Result<Option<DatabaseInfo>> {
        Ok(self.databases.get(name).cloned())
    }

    fn list_databases(&self) -> Result<Vec<String>> {
        Ok(self.databases.keys().cloned().collect())
    }

    fn create_retention_policy(&mut self, db: &str, name: &str, duration: u64, replica: u32) -> Result<()> {
        let rp = crate::schema::RetentionPolicy::new(name.to_string(), duration)
            .with_replica_count(replica);
        self.retention_policies
            .entry(db.to_string())
            .or_default()
            .insert(name.to_string(), rp);
        Ok(())
    }

    fn get_retention_policy(&self, db: &str, name: &str) -> Result<Option<crate::schema::RetentionPolicy>> {
        Ok(self.retention_policies
            .get(db)
            .and_then(|rps| rps.get(name).cloned()))
    }

    fn create_measurement(&mut self, _db: &str, _rp: &str, _name: &str) -> Result<()> {
        Ok(())
    }

    fn update_shard_readers(&mut self, shard_mapping: &ShardMapping) -> Result<()> {
        self.shard_mappings.insert(shard_mapping.shard_id, shard_mapping.clone());
        Ok(())
    }

    fn get_shard_mappings(&self, db: &str, rp: &str, start: i64, end: i64) -> Result<Vec<ShardMapping>> {
        Ok(self
            .shard_mappings
            .values()
            .filter(|m| {
                m.database == db
                    && m.retention_policy == rp
                    && m.start_time <= end
                    && m.end_time >= start
            })
            .cloned()
            .collect())
    }

    fn get_shard_owner(&self, shard_id: u64) -> Result<Option<ShardMapping>> {
        Ok(self.shard_mappings.get(&shard_id).cloned())
    }

    fn acquire_node(&self) -> Result<u64> {
        Ok(self.node_id)
    }

    fn node(&self) -> Result<NodeInfo> {
        Ok(NodeInfo::new(self.node_id, "127.0.0.1:8080".parse().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_info_new() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let node = NodeInfo::new(1, addr);
        
        assert_eq!(node.id, 1);
        assert!(!node.is_leader);
        assert_eq!(node.term, 0);
    }

    #[test]
    fn test_node_info_with_options() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let node = NodeInfo::new(1, addr)
            .with_leader(true)
            .with_term(5);
        
        assert!(node.is_leader);
        assert_eq!(node.term, 5);
    }

    #[test]
    fn test_shard_mapping_new() {
        let mapping = ShardMapping::new(1, 100, "testdb", "rp1", 0, 3600);
        
        assert_eq!(mapping.shard_id, 1);
        assert_eq!(mapping.node_id, 100);
        assert_eq!(mapping.database, "testdb");
        assert_eq!(mapping.retention_policy, "rp1");
    }

    #[test]
    fn test_meta_client_stub_new() {
        let peers = vec![NodeInfo::new(1, "127.0.0.1:8080".parse().unwrap())];
        let stub = MetaClientStub::new(1, peers);
        
        assert_eq!(stub.node_id(), 1);
        assert!(stub.peers().len() == 1);
    }

    #[test]
    fn test_meta_client_stub_create_database() {
        let peers = vec![NodeInfo::new(1, "127.0.0.1:8080".parse().unwrap())];
        let mut stub = MetaClientStub::new(1, peers);
        
        let db = stub.create_database("testdb").unwrap();
        assert_eq!(db.name, "testdb");
    }

    #[test]
    fn test_meta_client_stub_get_database() {
        let peers = vec![NodeInfo::new(1, "127.0.0.1:8080".parse().unwrap())];
        let mut stub = MetaClientStub::new(1, peers);
        
        stub.create_database("testdb").unwrap();
        
        let db = stub.get_database("testdb").unwrap();
        assert!(db.is_some());
        assert_eq!(db.unwrap().name, "testdb");
    }

    #[test]
    fn test_meta_client_stub_create_retention_policy() {
        let peers = vec![NodeInfo::new(1, "127.0.0.1:8080".parse().unwrap())];
        let mut stub = MetaClientStub::new(1, peers);
        
        stub.create_retention_policy("testdb", "rp1", 86400, 1).unwrap();
        
        let rp = stub.get_retention_policy("testdb", "rp1").unwrap();
        assert!(rp.is_some());
        assert_eq!(rp.unwrap().name, "rp1");
    }

    #[test]
    fn test_meta_client_stub_update_shard_readers() {
        let peers = vec![NodeInfo::new(1, "127.0.0.1:8080".parse().unwrap())];
        let mut stub = MetaClientStub::new(1, peers);
        
        let mapping = ShardMapping::new(1, 100, "testdb", "rp1", 0, 3600);
        stub.update_shard_readers(&mapping).unwrap();
        
        let owner = stub.get_shard_owner(1).unwrap();
        assert!(owner.is_some());
        assert_eq!(owner.unwrap().node_id, 100);
    }

    #[test]
    fn test_meta_client_stub_get_shard_mappings() {
        let peers = vec![NodeInfo::new(1, "127.0.0.1:8080".parse().unwrap())];
        let mut stub = MetaClientStub::new(1, peers);
        
        let mapping = ShardMapping::new(1, 100, "testdb", "rp1", 0, 3600);
        stub.update_shard_readers(&mapping).unwrap();
        
        let mappings = stub.get_shard_mappings("testdb", "rp1", 100, 3500).unwrap();
        assert_eq!(mappings.len(), 1);
    }
}
