use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::thread;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum TaskPriority {
    High,
    Medium,
    Low,
}

impl TaskPriority {
    #[allow(dead_code)]
    fn as_usize(&self) -> usize {
        match self {
            TaskPriority::High => 0,
            TaskPriority::Medium => 1,
            TaskPriority::Low => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TaskId(u64);

impl TaskId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub id: TaskId,
    pub name: String,
    pub priority: TaskPriority,
    pub interval: Duration,
    pub enabled: bool,
}

impl ScheduledTask {
    pub fn new(name: String, interval: Duration) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            id: TaskId(id),
            name,
            priority: TaskPriority::Medium,
            interval,
            enabled: true,
        }
    }

    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn enable(&mut self) {
        self.enabled = true;
    }

    pub fn disable(&mut self) {
        self.enabled = false;
    }
}

#[derive(Clone)]
pub struct TaskExecution {
    pub task_id: TaskId,
    pub started_at: Instant,
    pub completed_at: Option<Instant>,
    pub success: bool,
    pub error: Option<String>,
}

impl TaskExecution {
    pub fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            started_at: Instant::now(),
            completed_at: None,
            success: true,
            error: None,
        }
    }

    pub fn finish(&mut self, success: bool, error: Option<String>) {
        self.completed_at = Some(Instant::now());
        self.success = success;
        self.error = error;
    }

    pub fn duration(&self) -> Duration {
        let end = self.completed_at.unwrap_or_else(Instant::now);
        end.duration_since(self.started_at)
    }
}

pub trait TaskHandler: Send + Sync {
    fn name(&self) -> &str;
    fn execute(&self) -> Result<(), String>;
}

struct TaskWrapper {
    task: ScheduledTask,
    handler: Arc<dyn TaskHandler>,
    last_run: Option<Instant>,
    next_run: Option<Instant>,
}

impl TaskWrapper {
    fn new(task: ScheduledTask, handler: Arc<dyn TaskHandler>) -> Self {
        Self {
            task,
            handler,
            last_run: None,
            next_run: Some(Instant::now()),
        }
    }

    #[allow(dead_code)]
    fn should_run(&self) -> bool {
        if !self.task.enabled {
            return false;
        }
        if let Some(next_run) = self.next_run {
            return Instant::now() >= next_run;
        }
        true
    }

    fn run(&mut self) -> TaskExecution {
        let mut execution = TaskExecution::new(self.task.id.clone());
        
        match self.handler.execute() {
            Ok(()) => {
                execution.finish(true, None);
            }
            Err(e) => {
                execution.finish(false, Some(e));
            }
        }
        
        self.last_run = Some(Instant::now());
        self.next_run = Some(Instant::now() + self.task.interval);
        
        execution
    }
}

#[allow(dead_code)]
pub struct Scheduler {
    tasks: RwLock<HashMap<u64, TaskWrapper>>,
    executions: RwLock<Vec<TaskExecution>>,
    max_execution_history: usize,
    running: Arc<RwLock<bool>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            executions: RwLock::new(Vec::new()),
            max_execution_history: 1000,
            running: Arc::new(RwLock::new(false)),
        }
    }

    pub fn start(&self, poll_interval: Duration) -> thread::JoinHandle<()> {
        {
            let mut running = self.running.write().unwrap();
            if *running {
                panic!("Scheduler already running");
            }
            *running = true;
        }

        let running = self.running.clone();
        
        thread::spawn(move || {
            loop {
                thread::sleep(poll_interval);
                
                {
                    let running = running.read().unwrap();
                    if !*running {
                        break;
                    }
                }
            }
        })
    }

    pub fn stop(&self) {
        *self.running.write().unwrap() = false;
    }

    pub fn add_task(&self, task: ScheduledTask, handler: Arc<dyn TaskHandler>) {
        let wrapper = TaskWrapper::new(task, handler);
        self.tasks.write().unwrap().insert(wrapper.task.id.0, wrapper);
    }

    pub fn remove_task(&self, task_id: TaskId) -> bool {
        self.tasks.write().unwrap().remove(&task_id.0).is_some()
    }

    pub fn get_task(&self, task_id: TaskId) -> Option<ScheduledTask> {
        self.tasks.read().unwrap()
            .get(&task_id.0)
            .map(|w| w.task.clone())
    }

    pub fn list_tasks(&self) -> Vec<ScheduledTask> {
        self.tasks.read().unwrap()
            .values()
            .map(|w| w.task.clone())
            .collect()
    }

    pub fn enable_task(&self, task_id: TaskId) -> bool {
        if let Some(wrapper) = self.tasks.write().unwrap().get_mut(&task_id.0) {
            wrapper.task.enable();
            true
        } else {
            false
        }
    }

    pub fn disable_task(&self, task_id: TaskId) -> bool {
        if let Some(wrapper) = self.tasks.write().unwrap().get_mut(&task_id.0) {
            wrapper.task.disable();
            true
        } else {
            false
        }
    }

    pub fn trigger_task(&self, task_id: TaskId) -> Option<TaskExecution> {
        let execution = {
            let mut binding = self.tasks.write().unwrap();
            let wrapper = binding.get_mut(&task_id.0)?;
            wrapper.run()
        };
        
        self.executions.write().unwrap().push(execution.clone());
        Some(execution)
    }

    pub fn get_executions(&self, task_id: Option<TaskId>) -> Vec<TaskExecution> {
        let executions = self.executions.read().unwrap().clone();
        match task_id {
            Some(id) => executions.iter()
                .filter(|e| e.task_id.0 == id.0)
                .cloned()
                .collect(),
            None => executions,
        }
    }

    pub fn task_count(&self) -> usize {
        self.tasks.read().unwrap().len()
    }

    pub fn is_running(&self) -> bool {
        *self.running.read().unwrap()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("task_count", &self.task_count())
            .field("running", &self.is_running())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;

    struct TestHandler {
        name: String,
        call_count: AtomicUsize,
        should_fail: AtomicBool,
    }

    impl TestHandler {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                call_count: AtomicUsize::new(0),
                should_fail: AtomicBool::new(false),
            }
        }

        fn set_should_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::SeqCst);
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    impl TaskHandler for TestHandler {
        fn name(&self) -> &str {
            &self.name
        }

        fn execute(&self) -> Result<(), String> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if self.should_fail.load(Ordering::SeqCst) {
                Err("Test error".to_string())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn test_scheduler_creation() {
        let scheduler = Scheduler::new();
        assert_eq!(scheduler.task_count(), 0);
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_add_task() {
        let scheduler = Scheduler::new();
        let handler = StdArc::new(TestHandler::new("test_task"));
        let task = ScheduledTask::new("test_task".to_string(), Duration::from_secs(60));
        
        scheduler.add_task(task, handler);
        
        assert_eq!(scheduler.task_count(), 1);
    }

    #[test]
    fn test_remove_task() {
        let scheduler = Scheduler::new();
        let handler = StdArc::new(TestHandler::new("test_task"));
        let task = ScheduledTask::new("test_task".to_string(), Duration::from_secs(60));
        
        scheduler.add_task(task, handler.clone());
        assert_eq!(scheduler.task_count(), 1);
        
        let tasks = scheduler.list_tasks();
        let task_id = tasks[0].id.clone();
        
        assert!(scheduler.remove_task(task_id));
        assert_eq!(scheduler.task_count(), 0);
    }

    #[test]
    fn test_enable_disable_task() {
        let scheduler = Scheduler::new();
        let handler = StdArc::new(TestHandler::new("test_task"));
        let task = ScheduledTask::new("test_task".to_string(), Duration::from_secs(60));
        
        scheduler.add_task(task, handler);
        
        let tasks = scheduler.list_tasks();
        let task_id = tasks[0].id.clone();
        
        assert!(scheduler.disable_task(task_id.clone()));
        
        let task = scheduler.get_task(task_id.clone()).unwrap();
        assert!(!task.enabled);
        
        assert!(scheduler.enable_task(task_id.clone()));
        
        let task = scheduler.get_task(task_id).unwrap();
        assert!(task.enabled);
    }

    #[test]
    fn test_trigger_task() {
        let scheduler = Scheduler::new();
        let handler = StdArc::new(TestHandler::new("test_task"));
        let task = ScheduledTask::new("test_task".to_string(), Duration::from_secs(60));
        
        scheduler.add_task(task, handler.clone());
        
        let tasks = scheduler.list_tasks();
        let task_id = tasks[0].id.clone();
        
        assert_eq!(handler.call_count(), 0);
        
        let execution = scheduler.trigger_task(task_id);
        assert!(execution.is_some());
        assert!(execution.unwrap().success);
        
        assert_eq!(handler.call_count(), 1);
    }

    #[test]
    fn test_task_with_priority() {
        let scheduler = Scheduler::new();
        let handler = StdArc::new(TestHandler::new("test_task"));
        let task = ScheduledTask::new("test_task".to_string(), Duration::from_secs(60))
            .with_priority(TaskPriority::High);
        
        scheduler.add_task(task, handler);
        
        let tasks = scheduler.list_tasks();
        assert_eq!(tasks[0].priority, TaskPriority::High);
    }

    #[test]
    fn test_failed_task_execution() {
        let scheduler = Scheduler::new();
        let handler = StdArc::new(TestHandler::new("test_task"));
        handler.set_should_fail(true);
        
        let task = ScheduledTask::new("test_task".to_string(), Duration::from_secs(60));
        scheduler.add_task(task, handler.clone());
        
        let tasks = scheduler.list_tasks();
        let task_id = tasks[0].id.clone();
        
        let execution = scheduler.trigger_task(task_id).unwrap();
        assert!(!execution.success);
        assert!(execution.error.is_some());
    }

    #[test]
    fn test_get_task_executions() {
        let scheduler = Scheduler::new();
        let handler = StdArc::new(TestHandler::new("test_task"));
        let task = ScheduledTask::new("test_task".to_string(), Duration::from_secs(60));
        
        scheduler.add_task(task, handler.clone());
        
        let tasks = scheduler.list_tasks();
        let task_id = tasks[0].id.clone();
        
        scheduler.trigger_task(task_id.clone()).unwrap();
        scheduler.trigger_task(task_id.clone()).unwrap();
        
        let executions = scheduler.get_executions(Some(task_id));
        assert_eq!(executions.len(), 2);
    }

    #[test]
    fn test_list_tasks() {
        let scheduler = Scheduler::new();
        
        let handler1 = StdArc::new(TestHandler::new("task1"));
        let handler2 = StdArc::new(TestHandler::new("task2"));
        
        scheduler.add_task(ScheduledTask::new("task1".to_string(), Duration::from_secs(60)), handler1);
        scheduler.add_task(ScheduledTask::new("task2".to_string(), Duration::from_secs(120)), handler2);
        
        let tasks = scheduler.list_tasks();
        assert_eq!(tasks.len(), 2);
        
        let names: Vec<&str> = tasks.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"task1"));
        assert!(names.contains(&"task2"));
    }

    #[test]
    fn test_task_id_uniqueness() {
        let scheduler = Scheduler::new();
        let handler = StdArc::new(TestHandler::new("test"));
        
        let task1 = ScheduledTask::new("task".to_string(), Duration::from_secs(60));
        let task2 = ScheduledTask::new("task".to_string(), Duration::from_secs(60));
        
        scheduler.add_task(task1, handler.clone());
        scheduler.add_task(task2, handler);
        
        let tasks = scheduler.list_tasks();
        assert_eq!(tasks.len(), 2);
        assert_ne!(tasks[0].id.0, tasks[1].id.0);
    }
}