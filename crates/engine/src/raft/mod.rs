use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

impl NodeState {
    pub fn is_leader(&self) -> bool {
        matches!(self, NodeState::Leader)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VoteResponse {
    Granted,
    Denied,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResult {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResult {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub data: LogEntryData,
    pub timestamp: i64,
}

impl LogEntry {
    pub fn new(index: u64, term: u64, data: LogEntryData) -> Self {
        Self {
            index,
            term,
            data,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LogEntryData {
    #[serde(rename = "config")]
    Config { command: ConfigCommand },
    #[serde(rename = "data")]
    Data { database: String, payload: Vec<u8> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigCommand {
    AddNode { node_id: u64, addr: SocketAddr },
    RemoveNode { node_id: u64 },
    AddReplica { shard_id: u64, node_ids: Vec<u64> },
    RemoveReplica { shard_id: u64, node_ids: Vec<u64> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct Membership {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub state: NodeState,
}

impl NodeInfo {
    pub fn new(id: u64, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            state: NodeState::Follower,
        }
    }

    pub fn with_state(mut self, state: NodeState) -> Self {
        self.state = state;
        self
    }
}

pub struct RaftNode {
    node_id: u64,
    current_term: RwLock<u64>,
    voted_for: RwLock<Option<u64>>,
    state: RwLock<NodeState>,
    log: RwLock<Vec<LogEntry>>,
    commit_index: RwLock<u64>,
    last_applied: RwLock<u64>,
    membership: RwLock<Membership>,
    peers: RwLock<HashMap<u64, NodeInfo>>,
    election_timeout_ms: u64,
    heartbeat_interval_ms: u64,
}

impl PartialEq for RaftNode {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Eq for RaftNode {}

impl RaftNode {
    pub fn new(node_id: u64, election_timeout_ms: u64, heartbeat_interval_ms: u64) -> Self {
        Self {
            node_id,
            current_term: RwLock::new(0),
            voted_for: RwLock::new(None),
            state: RwLock::new(NodeState::Follower),
            log: RwLock::new(Vec::new()),
            commit_index: RwLock::new(0),
            last_applied: RwLock::new(0),
            membership: RwLock::new(Membership::default()),
            peers: RwLock::new(HashMap::new()),
            election_timeout_ms,
            heartbeat_interval_ms,
        }
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn current_term(&self) -> u64 {
        *self.current_term.read().unwrap()
    }

    pub fn state(&self) -> NodeState {
        *self.state.read().unwrap()
    }

    pub fn is_leader(&self) -> bool {
        self.state.read().unwrap().is_leader()
    }

    pub fn add_peer(&self, peer: NodeInfo) {
        self.peers.write().unwrap().insert(peer.id, peer);
    }

    pub fn remove_peer(&self, peer_id: u64) {
        self.peers.write().unwrap().remove(&peer_id);
    }

    pub fn get_peers(&self) -> Vec<NodeInfo> {
        self.peers.read().unwrap().values().cloned().collect()
    }

    pub fn get_membership(&self) -> Membership {
        self.membership.read().unwrap().clone()
    }

    pub fn set_membership(&self, membership: Membership) {
        *self.membership.write().unwrap() = membership;
    }

    pub fn append_log(&self, entry: LogEntry) -> u64 {
        let mut log = self.log.write().unwrap();
        let index = entry.index;
        log.push(entry);
        index
    }

    pub fn get_log(&self) -> Vec<LogEntry> {
        self.log.read().unwrap().clone()
    }

    pub fn get_log_entry(&self, index: u64) -> Option<LogEntry> {
        self.log.read().unwrap().iter().find(|e| e.index == index).cloned()
    }

    pub fn last_log_index(&self) -> u64 {
        let log = self.log.read().unwrap();
        log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        let log = self.log.read().unwrap();
        log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn commit_index(&self) -> u64 {
        *self.commit_index.read().unwrap()
    }

    pub fn set_commit_index(&self, index: u64) {
        *self.commit_index.write().unwrap() = index;
    }

    pub fn last_applied(&self) -> u64 {
        *self.last_applied.read().unwrap()
    }

    pub fn set_last_applied(&self, index: u64) {
        *self.last_applied.write().unwrap() = index;
    }

    pub fn become_leader(&self) {
        let mut state = self.state.write().unwrap();
        *state = NodeState::Leader;
        
        let mut current_term = self.current_term.write().unwrap();
        *current_term += 1;
    }

    pub fn become_follower(&self, term: u64) {
        let mut state = self.state.write().unwrap();
        *state = NodeState::Follower;
        
        let mut current_term = self.current_term.write().unwrap();
        *current_term = term;
    }

    pub fn become_candidate(&self) {
        let mut state = self.state.write().unwrap();
        *state = NodeState::Candidate;
        
        let mut current_term = self.current_term.write().unwrap();
        *current_term += 1;
    }

    pub fn request_vote(&self, req: VoteRequest) -> VoteResult {
        let current_term = *self.current_term.read().unwrap();
        
        if req.term < current_term {
            return VoteResult {
                term: current_term,
                vote_granted: false,
            };
        }
        
        let mut voted_for = self.voted_for.write().unwrap();
        
        if (voted_for.is_none() || *voted_for == Some(req.candidate_id))
            && req.last_log_index >= self.last_log_index() && req.last_log_term >= self.last_log_term() {
                *voted_for = Some(req.candidate_id);
                return VoteResult {
                    term: req.term,
                    vote_granted: true,
                };
            }
        
        VoteResult {
            term: current_term,
            vote_granted: false,
        }
    }

    pub fn append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResult {
        let current_term = *self.current_term.read().unwrap();
        
        if req.term < current_term {
            return AppendEntriesResult {
                term: current_term,
                success: false,
                match_index: 0,
            };
        }
        
        {
            let log = self.log.read().unwrap();
            if req.prev_log_index > 0 && (req.prev_log_index as usize) > log.len() {
                return AppendEntriesResult {
                    term: current_term,
                    success: false,
                    match_index: 0,
                };
            }
        }
        
        let mut log = self.log.write().unwrap();
        
        if req.prev_log_index > 0 {
            if (req.prev_log_index as usize) >= log.len() {
                log.truncate(req.prev_log_index as usize);
            } else if log[req.prev_log_index as usize].term != req.prev_log_term {
                log.truncate(req.prev_log_index as usize);
            }
        }
        
        for entry in req.entries {
            let index = entry.index as usize;
            if index < log.len() {
                log[index] = entry;
            } else {
                log.push(entry);
            }
        }
        
        let match_index = log.last().map(|e| e.index).unwrap_or(0);
        
        if req.leader_commit > *self.commit_index.read().unwrap() {
            *self.commit_index.write().unwrap() = req.leader_commit.min(match_index);
        }
        
        AppendEntriesResult {
            term: current_term,
            success: true,
            match_index,
        }
    }

    pub fn election_timeout_ms(&self) -> u64 {
        self.election_timeout_ms
    }

    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.heartbeat_interval_ms
    }

    pub fn log_len(&self) -> usize {
        self.log.read().unwrap().len()
    }
}

pub struct RaftCluster {
    nodes: RwLock<HashMap<u64, Arc<RaftNode>>>,
    local_node: RwLock<Option<Arc<RaftNode>>>,
}

impl RaftCluster {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            local_node: RwLock::new(None),
        }
    }

    pub fn create_node(&self, node_id: u64, election_timeout_ms: u64, heartbeat_interval_ms: u64) -> Arc<RaftNode> {
        let node = Arc::new(RaftNode::new(node_id, election_timeout_ms, heartbeat_interval_ms));
        self.nodes.write().unwrap().insert(node_id, node.clone());
        node
    }

    pub fn get_node(&self, node_id: u64) -> Option<Arc<RaftNode>> {
        self.nodes.read().unwrap().get(&node_id).cloned()
    }

    pub fn remove_node(&self, node_id: u64) -> bool {
        self.nodes.write().unwrap().remove(&node_id).is_some()
    }

    pub fn list_nodes(&self) -> Vec<u64> {
        self.nodes.read().unwrap().keys().cloned().collect()
    }

    pub fn set_local_node(&self, node: Arc<RaftNode>) {
        *self.local_node.write().unwrap() = Some(node);
    }

    pub fn get_local_node(&self) -> Option<Arc<RaftNode>> {
        self.local_node.read().unwrap().clone()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.read().unwrap().len()
    }
}

impl Default for RaftCluster {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn new_socket_addr(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }

    #[test]
    fn test_node_state_is_leader() {
        assert!(NodeState::Leader.is_leader());
        assert!(!NodeState::Follower.is_leader());
        assert!(!NodeState::Candidate.is_leader());
        assert!(!NodeState::PreCandidate.is_leader());
    }

    #[test]
    fn test_raft_node_creation() {
        let node = RaftNode::new(1, 5000, 1000);
        assert_eq!(node.node_id(), 1);
        assert_eq!(node.current_term(), 0);
        assert_eq!(node.state(), NodeState::Follower);
        assert!(!node.is_leader());
    }

    #[test]
    fn test_add_remove_peer() {
        let node = RaftNode::new(1, 5000, 1000);
        let peer = NodeInfo::new(2, new_socket_addr(8080));
        
        node.add_peer(peer.clone());
        assert_eq!(node.get_peers().len(), 1);
        
        node.remove_peer(2);
        assert_eq!(node.get_peers().len(), 0);
    }

    #[test]
    fn test_append_log() {
        let node = RaftNode::new(1, 5000, 1000);
        
        let entry = LogEntry::new(1, 1, LogEntryData::Data {
            database: "testdb".to_string(),
            payload: vec![1, 2, 3],
        });
        
        let index = node.append_log(entry);
        assert_eq!(index, 1);
        assert_eq!(node.last_log_index(), 1);
        assert_eq!(node.last_log_term(), 1);
    }

    #[test]
    fn test_log_entry() {
        let entry = LogEntry::new(1, 1, LogEntryData::Data {
            database: "testdb".to_string(),
            payload: vec![1, 2, 3],
        });
        
        assert_eq!(entry.index, 1);
        assert_eq!(entry.term, 1);
        assert!(entry.timestamp > 0);
    }

    #[test]
    fn test_become_leader() {
        let node = RaftNode::new(1, 5000, 1000);
        
        node.become_leader();
        assert_eq!(node.state(), NodeState::Leader);
        assert!(node.is_leader());
        assert_eq!(node.current_term(), 1);
    }

    #[test]
    fn test_become_follower() {
        let node = RaftNode::new(1, 5000, 1000);
        node.become_candidate();
        node.become_follower(5);
        
        assert_eq!(node.state(), NodeState::Follower);
        assert_eq!(node.current_term(), 5);
    }

    #[test]
    fn test_request_vote() {
        let node = RaftNode::new(1, 5000, 1000);
        
        let req = VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        
        let result = node.request_vote(req);
        assert!(result.vote_granted);
        assert_eq!(result.term, 1);
    }

    #[test]
    fn test_request_vote_denied_wrong_term() {
        let node = RaftNode::new(1, 5000, 1000);
        node.become_leader();
        
        let req = VoteRequest {
            term: 0,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        
        let result = node.request_vote(req);
        assert!(!result.vote_granted);
    }

    #[test]
    fn test_append_entries() {
        let node = RaftNode::new(1, 5000, 1000);
        
        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry::new(1, 1, LogEntryData::Data {
                database: "testdb".to_string(),
                payload: vec![],
            })],
            leader_commit: 1,
        };
        
        let result = node.append_entries(req);
        assert!(result.success);
        assert_eq!(node.last_log_index(), 1);
    }

    #[test]
    fn test_append_entries_fail_wrong_term() {
        let node = RaftNode::new(1, 5000, 1000);
        node.become_leader();
        
        let req = AppendEntriesRequest {
            term: 0,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        
        let result = node.append_entries(req);
        assert!(!result.success);
    }

    #[test]
    fn test_raft_cluster() {
        let cluster = RaftCluster::new();
        
        let node = cluster.create_node(1, 5000, 1000);
        cluster.set_local_node(node.clone());
        
        assert_eq!(cluster.node_count(), 1);
        assert!(cluster.get_local_node().is_some());
        assert_eq!(cluster.get_local_node().unwrap().node_id(), 1);
        
        cluster.create_node(2, 5000, 1000);
        assert_eq!(cluster.node_count(), 2);
        
        cluster.remove_node(2);
        assert_eq!(cluster.node_count(), 1);
    }

    #[test]
    fn test_membership() {
        let node = RaftNode::new(1, 5000, 1000);
        
        let membership = Membership {
            voters: vec![1, 2, 3],
            learners: vec![4, 5],
        };
        
        node.set_membership(membership.clone());
        let retrieved = node.get_membership();
        
        assert_eq!(retrieved.voters, vec![1, 2, 3]);
        assert_eq!(retrieved.learners, vec![4, 5]);
    }

    #[test]
    fn test_commit_and_apply() {
        let node = RaftNode::new(1, 5000, 1000);
        
        for i in 1..=5 {
            node.append_log(LogEntry::new(i, 1, LogEntryData::Data {
                database: "testdb".to_string(),
                payload: vec![],
            }));
        }
        
        node.set_commit_index(3);
        assert_eq!(node.commit_index(), 3);
        
        node.set_last_applied(3);
        assert_eq!(node.last_applied(), 3);
    }

    #[test]
    fn test_get_log_entry() {
        let node = RaftNode::new(1, 5000, 1000);

        node.append_log(LogEntry::new(1, 1, LogEntryData::Data {
            database: "testdb".to_string(),
            payload: vec![],
        }));

        let entry = node.get_log_entry(1);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().index, 1);

        let none = node.get_log_entry(999);
        assert!(none.is_none());
    }

    #[test]
    fn test_vote_request_stale_term() {
        let node = RaftNode::new(1, 5000, 1000);
        node.become_leader();

        let req = VoteRequest {
            term: 0,
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: 1,
        };

        let result = node.request_vote(req);
        assert!(!result.vote_granted);
    }

    #[test]
    fn test_vote_request_already_voted() {
        let node = RaftNode::new(1, 5000, 1000);

        let req1 = VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        node.request_vote(req1);

        let req2 = VoteRequest {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        };
        let result = node.request_vote(req2);
        assert!(!result.vote_granted);
    }

    #[test]
    fn test_vote_request_outdated_log() {
        let node = RaftNode::new(1, 5000, 1000);

        node.append_log(LogEntry::new(1, 2, LogEntryData::Data {
            database: "testdb".to_string(),
            payload: vec![],
        }));

        let req = VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let result = node.request_vote(req);
        assert!(!result.vote_granted);
    }

    #[test]
    fn test_append_entries_truncate_conflict() {
        let node = RaftNode::new(1, 5000, 1000);

        node.append_log(LogEntry::new(1, 1, LogEntryData::Data {
            database: "testdb".to_string(),
            payload: vec![1],
        }));
        node.append_log(LogEntry::new(2, 1, LogEntryData::Data {
            database: "testdb".to_string(),
            payload: vec![2],
        }));

        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry::new(1, 2, LogEntryData::Data {
                    database: "testdb".to_string(),
                    payload: vec![10],
                }),
                LogEntry::new(2, 2, LogEntryData::Data {
                    database: "testdb".to_string(),
                    payload: vec![20],
                }),
            ],
            leader_commit: 2,
        };

        let result = node.append_entries(req);
        assert!(result.success);
        assert_eq!(node.last_log_index(), 2);
    }

    #[test]
    fn test_append_entries_update_commit_index() {
        let node = RaftNode::new(1, 5000, 1000);

        node.append_log(LogEntry::new(1, 1, LogEntryData::Data {
            database: "testdb".to_string(),
            payload: vec![],
        }));

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 1,
        };

        node.append_entries(req);
        assert_eq!(node.commit_index(), 1);
    }

    #[test]
    fn test_append_entries_no_update_if_lower_commit() {
        let node = RaftNode::new(1, 5000, 1000);
        node.set_commit_index(5);

        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 3,
        };

        node.append_entries(req);
        assert_eq!(node.commit_index(), 5);
    }

    #[test]
    fn test_become_candidate() {
        let node = RaftNode::new(1, 5000, 1000);
        assert_eq!(node.state(), NodeState::Follower);
        assert_eq!(node.current_term(), 0);

        node.become_candidate();
        assert_eq!(node.state(), NodeState::Candidate);
        assert_eq!(node.current_term(), 1);
    }

    #[test]
    fn test_multiple_term_increments() {
        let node = RaftNode::new(1, 5000, 1000);

        node.become_candidate();
        node.become_candidate();
        node.become_candidate();

        assert_eq!(node.current_term(), 3);
        assert_eq!(node.state(), NodeState::Candidate);
    }

    #[test]
    fn test_log_entries_persist_across_state_changes() {
        let node = RaftNode::new(1, 5000, 1000);

        node.append_log(LogEntry::new(1, 1, LogEntryData::Data {
            database: "db1".to_string(),
            payload: vec![1],
        }));
        node.append_log(LogEntry::new(2, 1, LogEntryData::Data {
            database: "db2".to_string(),
            payload: vec![2],
        }));

        node.become_leader();
        assert_eq!(node.last_log_index(), 2);

        node.become_follower(2);
        assert_eq!(node.last_log_index(), 2);
    }

    #[test]
    fn test_get_log_returns_copy() {
        let node = RaftNode::new(1, 5000, 1000);

        node.append_log(LogEntry::new(1, 1, LogEntryData::Data {
            database: "testdb".to_string(),
            payload: vec![],
        }));

        let log1 = node.get_log();
        let log2 = node.get_log();

        assert_eq!(log1.len(), log2.len());
    }

    #[test]
    fn test_peers_management() {
        let node = RaftNode::new(1, 5000, 1000);

        let peer1 = NodeInfo::new(2, new_socket_addr(8080));
        let peer2 = NodeInfo::new(3, new_socket_addr(8081));

        node.add_peer(peer1.clone());
        node.add_peer(peer2.clone());

        let peers = node.get_peers();
        assert_eq!(peers.len(), 2);

        node.remove_peer(2);
        let peers = node.get_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, 3);
    }

    #[test]
    fn test_vote_result_serialization() {
        let result = VoteResult {
            term: 5,
            vote_granted: true,
        };

        let serialized = serde_json::to_string(&result).unwrap();
        let deserialized: VoteResult = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.term, 5);
        assert!(deserialized.vote_granted);
    }

    #[test]
    fn test_append_entries_result_serialization() {
        let result = AppendEntriesResult {
            term: 3,
            success: true,
            match_index: 10,
        };

        let serialized = serde_json::to_string(&result).unwrap();
        let deserialized: AppendEntriesResult = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.term, 3);
        assert!(deserialized.success);
        assert_eq!(deserialized.match_index, 10);
    }

    #[test]
    fn test_membership_default() {
        let membership = Membership::default();
        assert!(membership.voters.is_empty());
        assert!(membership.learners.is_empty());
    }

    #[test]
    fn test_membership_with_nodes() {
        let membership = Membership {
            voters: vec![1, 2, 3],
            learners: vec![4, 5],
        };

        assert_eq!(membership.voters.len(), 3);
        assert_eq!(membership.learners.len(), 2);
    }

    #[test]
    fn test_node_info_with_state() {
        let node_info = NodeInfo::new(1, new_socket_addr(8080));
        assert_eq!(node_info.state, NodeState::Follower);

        let leader_info = node_info.with_state(NodeState::Leader);
        assert_eq!(leader_info.state, NodeState::Leader);
    }

    #[test]
    fn test_cluster_node_lifecycle() {
        let cluster = RaftCluster::new();

        let _node1 = cluster.create_node(1, 5000, 1000);
        let _node2 = cluster.create_node(2, 5000, 1000);

        assert_eq!(cluster.node_count(), 2);
        assert!(cluster.get_node(1).is_some());
        assert!(cluster.get_node(2).is_some());
        assert!(cluster.get_node(3).is_none());

        cluster.remove_node(2);
        assert_eq!(cluster.node_count(), 1);
    }

    #[test]
    fn test_cluster_local_node() {
        let cluster = RaftCluster::new();

        let node = cluster.create_node(1, 5000, 1000);
        cluster.set_local_node(node.clone());

        let local = cluster.get_local_node();
        assert!(local.is_some());
        assert_eq!(local.unwrap().node_id(), 1);
    }

    #[test]
    fn test_list_nodes() {
        let cluster = RaftCluster::new();

        cluster.create_node(1, 5000, 1000);
        cluster.create_node(2, 5000, 1000);
        cluster.create_node(3, 5000, 1000);

        let nodes = cluster.list_nodes();
        assert_eq!(nodes.len(), 3);
        assert!(nodes.contains(&1));
        assert!(nodes.contains(&2));
        assert!(nodes.contains(&3));
    }

    #[test]
    fn test_config_command_serialization() {
        let cmd = ConfigCommand::AddNode {
            node_id: 5,
            addr: new_socket_addr(9000),
        };

        let serialized = serde_json::to_string(&cmd).unwrap();
        assert!(serialized.contains("AddNode"));

        let cmd2 = ConfigCommand::RemoveReplica {
            shard_id: 1,
            node_ids: vec![2, 3],
        };

        let serialized2 = serde_json::to_string(&cmd2).unwrap();
        assert!(serialized2.contains("RemoveReplica"));
    }

    #[test]
    fn test_log_entry_data_serialization() {
        let data = LogEntryData::Config {
            command: ConfigCommand::AddNode {
                node_id: 1,
                addr: new_socket_addr(8080),
            },
        };

        let serialized = serde_json::to_string(&data).unwrap();
        assert!(serialized.contains("config"));

        let data2 = LogEntryData::Data {
            database: "mydb".to_string(),
            payload: vec![1, 2, 3],
        };

        let serialized2 = serde_json::to_string(&data2).unwrap();
        assert!(serialized2.contains("data"));
    }
}