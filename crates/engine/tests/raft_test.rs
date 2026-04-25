use openGemini_engine::raft::{
    AppendEntriesRequest, AppendEntriesResult, ConfigCommand, LogEntry, LogEntryData,
    Membership, NodeInfo, NodeState, RaftNode, VoteRequest, VoteResult, VoteResponse,
};
use std::net::SocketAddr;
use std::str::FromStr;

fn create_test_log_entry(index: u64, term: u64) -> LogEntry {
    LogEntry::new(index, term, LogEntryData::Data { database: "testdb".to_string(), payload: vec![] })
}

fn create_test_socket_addr() -> SocketAddr {
    SocketAddr::from_str("127.0.0.1:8080").unwrap()
}

fn create_test_node_info(id: u64) -> NodeInfo {
    NodeInfo {
        id,
        addr: create_test_socket_addr(),
        state: NodeState::Follower,
    }
}

#[test]
fn test_node_state_is_leader() {
    assert!(NodeState::Leader.is_leader());
    assert!(!NodeState::Follower.is_leader());
    assert!(!NodeState::Candidate.is_leader());
    assert!(!NodeState::PreCandidate.is_leader());
}

#[test]
fn test_node_state_variants() {
    assert_eq!(NodeState::Follower, NodeState::Follower);
    assert_eq!(NodeState::Candidate, NodeState::Candidate);
    assert_eq!(NodeState::Leader, NodeState::Leader);
    assert_eq!(NodeState::PreCandidate, NodeState::PreCandidate);
}

#[test]
fn test_raft_node_new() {
    let node = RaftNode::new(1, 1000, 500);

    assert_eq!(node.node_id(), 1);
    assert_eq!(node.current_term(), 0);
    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.commit_index(), 0);
    assert_eq!(node.last_applied(), 0);
}

#[test]
fn test_raft_node_is_leader() {
    let node = RaftNode::new(1, 1000, 500);

    assert!(!node.is_leader());

    node.become_leader();
    assert!(node.is_leader());
}

#[test]
fn test_raft_node_become_leader() {
    let node = RaftNode::new(1, 1000, 500);

    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), 0);

    node.become_leader();

    assert_eq!(node.state(), NodeState::Leader);
    assert_eq!(node.current_term(), 1);
}

#[test]
fn test_raft_node_become_follower() {
    let node = RaftNode::new(1, 1000, 500);
    node.become_leader();

    node.become_follower(2);

    assert_eq!(node.state(), NodeState::Follower);
    assert_eq!(node.current_term(), 2);
}

#[test]
fn test_raft_node_become_candidate() {
    let node = RaftNode::new(1, 1000, 500);

    node.become_candidate();

    assert_eq!(node.state(), NodeState::Candidate);
    assert_eq!(node.current_term(), 1);
}

#[test]
fn test_raft_node_add_peer() {
    let node = RaftNode::new(1, 1000, 500);
    let peer = create_test_node_info(2);

    node.add_peer(peer.clone());

    let peers = node.get_peers();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].id, 2);
}

#[test]
fn test_raft_node_remove_peer() {
    let node = RaftNode::new(1, 1000, 500);
    let peer = create_test_node_info(2);

    node.add_peer(peer);
    assert_eq!(node.get_peers().len(), 1);

    node.remove_peer(2);
    assert_eq!(node.get_peers().len(), 0);
}

#[test]
fn test_raft_node_get_peers_empty() {
    let node = RaftNode::new(1, 1000, 500);
    let peers = node.get_peers();
    assert!(peers.is_empty());
}

#[test]
fn test_raft_node_append_log() {
    let node = RaftNode::new(1, 1000, 500);
    let entry = create_test_log_entry(1, 1);

    let index = node.append_log(entry);

    assert_eq!(index, 1);
    assert_eq!(node.last_log_index(), 1);
}

#[test]
fn test_raft_node_get_log() {
    let node = RaftNode::new(1, 1000, 500);
    let entry1 = create_test_log_entry(1, 1);
    let entry2 = create_test_log_entry(2, 1);

    node.append_log(entry1);
    node.append_log(entry2);

    let log = node.get_log();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].index, 1);
    assert_eq!(log[1].index, 2);
}

#[test]
fn test_raft_node_get_log_empty() {
    let node = RaftNode::new(1, 1000, 500);
    let log = node.get_log();
    assert!(log.is_empty());
}

#[test]
fn test_raft_node_get_log_entry() {
    let node = RaftNode::new(1, 1000, 500);
    let entry = create_test_log_entry(1, 1);

    node.append_log(entry);

    let found = node.get_log_entry(1);
    assert!(found.is_some());
    assert_eq!(found.unwrap().index, 1);

    let not_found = node.get_log_entry(999);
    assert!(not_found.is_none());
}

#[test]
fn test_raft_node_last_log_index() {
    let node = RaftNode::new(1, 1000, 500);
    assert_eq!(node.last_log_index(), 0);

    node.append_log(create_test_log_entry(1, 1));
    node.append_log(create_test_log_entry(2, 1));
    assert_eq!(node.last_log_index(), 2);
}

#[test]
fn test_raft_node_last_log_term() {
    let node = RaftNode::new(1, 1000, 500);
    assert_eq!(node.last_log_term(), 0);

    node.append_log(create_test_log_entry(1, 1));
    node.append_log(create_test_log_entry(2, 2));
    assert_eq!(node.last_log_term(), 2);
}

#[test]
fn test_raft_node_commit_index() {
    let node = RaftNode::new(1, 1000, 500);
    assert_eq!(node.commit_index(), 0);

    node.set_commit_index(5);
    assert_eq!(node.commit_index(), 5);
}

#[test]
fn test_raft_node_last_applied() {
    let node = RaftNode::new(1, 1000, 500);
    assert_eq!(node.last_applied(), 0);

    node.set_last_applied(3);
    assert_eq!(node.last_applied(), 3);
}

#[test]
fn test_raft_node_membership() {
    let node = RaftNode::new(1, 1000, 500);
    let membership = Membership::default();

    node.set_membership(membership.clone());
    let retrieved = node.get_membership();

    assert_eq!(retrieved.voters.len(), membership.voters.len());
    assert_eq!(retrieved.learners.len(), membership.learners.len());
}

#[test]
fn test_log_entry_new() {
    let entry = LogEntry::new(1, 1, LogEntryData::Data { database: "test".to_string(), payload: vec![] });

    assert_eq!(entry.index, 1);
    assert_eq!(entry.term, 1);
    assert!(entry.timestamp > 0);
}

#[test]
fn test_vote_request_new() {
    let request = VoteRequest {
        term: 1,
        candidate_id: 1,
        last_log_index: 10,
        last_log_term: 1,
    };

    assert_eq!(request.term, 1);
    assert_eq!(request.candidate_id, 1);
    assert_eq!(request.last_log_index, 10);
    assert_eq!(request.last_log_term, 1);
}

#[test]
fn test_vote_result_new() {
    let result = VoteResult {
        term: 1,
        vote_granted: true,
    };

    assert_eq!(result.term, 1);
    assert!(result.vote_granted);
}

#[test]
fn test_append_entries_request_new() {
    let entries = vec![
        create_test_log_entry(1, 1),
        create_test_log_entry(2, 1),
    ];
    let request = AppendEntriesRequest {
        term: 1,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries,
        leader_commit: 2,
    };

    assert_eq!(request.term, 1);
    assert_eq!(request.leader_id, 1);
    assert_eq!(request.prev_log_index, 0);
    assert_eq!(request.leader_commit, 2);
    assert_eq!(request.entries.len(), 2);
}

#[test]
fn test_append_entries_result_new() {
    let result = AppendEntriesResult {
        term: 1,
        success: true,
        match_index: 2,
    };

    assert_eq!(result.term, 1);
    assert!(result.success);
    assert_eq!(result.match_index, 2);
}

#[test]
fn test_config_command_add_node() {
    let cmd = ConfigCommand::AddNode {
        node_id: 1,
        addr: create_test_socket_addr(),
    };

    match cmd {
        ConfigCommand::AddNode { node_id, .. } => assert_eq!(node_id, 1),
        _ => panic!("Expected AddNode"),
    }
}

#[test]
fn test_config_command_remove_node() {
    let cmd = ConfigCommand::RemoveNode { node_id: 1 };

    match cmd {
        ConfigCommand::RemoveNode { node_id } => assert_eq!(node_id, 1),
        _ => panic!("Expected RemoveNode"),
    }
}

#[test]
fn test_config_command_add_replica() {
    let cmd = ConfigCommand::AddReplica {
        shard_id: 1,
        node_ids: vec![1, 2, 3],
    };

    match cmd {
        ConfigCommand::AddReplica { shard_id, node_ids } => {
            assert_eq!(shard_id, 1);
            assert_eq!(node_ids.len(), 3);
        }
        _ => panic!("Expected AddReplica"),
    }
}

#[test]
fn test_config_command_remove_replica() {
    let cmd = ConfigCommand::RemoveReplica {
        shard_id: 1,
        node_ids: vec![1, 2],
    };

    match cmd {
        ConfigCommand::RemoveReplica { shard_id, node_ids } => {
            assert_eq!(shard_id, 1);
            assert_eq!(node_ids.len(), 2);
        }
        _ => panic!("Expected RemoveReplica"),
    }
}

#[test]
fn test_membership_default() {
    let membership = Membership::default();
    assert!(membership.voters.is_empty());
    assert!(membership.learners.is_empty());
}

#[test]
fn test_node_info() {
    let info = create_test_node_info(1);
    assert_eq!(info.id, 1);
    assert_eq!(info.addr, create_test_socket_addr());
}

#[test]
fn test_node_info_with_state() {
    let info = NodeInfo::new(1, create_test_socket_addr()).with_state(NodeState::Leader);
    assert_eq!(info.state, NodeState::Leader);
}

#[test]
fn test_vote_response_variants() {
    assert_eq!(VoteResponse::Granted, VoteResponse::Granted);
    assert_eq!(VoteResponse::Denied, VoteResponse::Denied);
}

#[test]
fn test_membership_with_voters() {
    let membership = Membership {
        voters: vec![1, 2, 3],
        learners: vec![],
    };
    assert_eq!(membership.voters.len(), 3);
    assert!(membership.learners.is_empty());
}

#[test]
fn test_membership_with_learners() {
    let membership = Membership {
        voters: vec![1, 2],
        learners: vec![3],
    };
    assert_eq!(membership.voters.len(), 2);
    assert_eq!(membership.learners.len(), 1);
}
