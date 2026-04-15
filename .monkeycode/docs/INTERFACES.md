# 接口定义

## 模块接口

### Engine 模块

```rust
// 存储引擎主入口
pub struct Engine {
    config: EngineConfig,
    wal: Wal,
    memtable: MemTable,
    tssp_manager: TsspManager,
    compaction: CompactionManager,
}

impl Engine {
    // 写入数据
    pub fn write(&self, batch: WriteBatch) -> Result<()>
    
    // 读取数据
    pub fn read(&self, query: Query) -> Result<QueryResult>
    
    // 刷盘（强制写入）
    pub fn flush(&self) -> Result<()>
    
    // 关闭引擎
    pub fn close(&self) -> Result<()>
}
```

### WAL 模块

```rust
pub struct Wal {
    dir: PathBuf,
    file_id: AtomicU64,
}

impl Wal {
    // 写入日志
    pub fn write(&self, entry: WalEntry) -> Result<u64>
    
    // 读取日志
    pub fn read(&self, offset: u64) -> Result<WalEntry>
    
    // 清理旧日志
    pub fn purge(&self, before: u64) -> Result<()>
}
```

### MemTable 模块

```rust
pub struct MemTable {
    data: BTreeMap<RowKey, RowValue>,
    size: AtomicU64,
    max_size: u64,
}

impl MemTable {
    // 写入一行
    pub fn insert(&self, key: RowKey, value: RowValue) -> Result<()>
    
    // 范围查询
    pub fn scan(&self, range: Range<RowKey>) -> Result<Vec<RowValue>>
    
    // 获取当前大小
    pub fn size(&self) -> u64
    
    // 是否需要刷盘
    pub fn should_flush(&self) -> bool
}
```

### TSSP 模块

```rust
pub struct TsspFile {
    path: PathBuf,
    meta: FileMeta,
}

pub struct TsspReader {
    file: TsspFile,
    column_cache: HashMap<u32, ColumnData>,
}

impl TsspReader {
    // 读取指定时间范围的数据
    pub fn read(&self, time_range: TimeRange, columns: &[u32]) -> Result<ColumnarResult>
    
    // 获取文件元数据
    pub fn meta(&self) -> &FileMeta
}

pub struct TsspWriter {
    file: TsspFile,
    schema: TableSchema,
}

impl TsspWriter {
    // 写入列数据
    pub fn write_column(&mut self, column_id: u32, data: ColumnData) -> Result<()>
    
    // 关闭文件
    pub fn close(self) -> Result<FileMeta>
}
```

### Compaction 模块

```rust
pub struct CompactionManager {
    config: CompactionConfig,
}

impl CompactionManager {
    // 执行合并任务
    pub fn compact(&self, inputs: Vec<TsspFile>, output: PathBuf) -> Result<()>
    
    // 检查是否需要合并
    pub fn needs_compaction(&self) -> bool
}
```

### Index 模块

```rust
pub struct SeriesIndex {
    bitmap: RoaringBitmap,
}

impl SeriesIndex {
    // 添加序列
    pub fn add(&mut self, series_id: u64)
    
    // 查找序列
    pub fn contains(&self, series_id: u64) -> bool
    
    // 范围查询
    pub fn range(&self, start: u64, end: u64) -> RoaringBitmap
}
```

## 数据结构

### WriteBatch

```rust
pub struct WriteBatch {
    pub database: String,
    pub table: String,
    pub rows: Vec<Row>,
    pub timestamp: i64,
}

pub struct Row {
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, FieldValue>,
}
```

### Query

```rust
pub struct Query {
    pub database: String,
    pub table: String,
    pub time_range: TimeRange,
    pub columns: Vec<String>,
    pub filter: Option<FilterExpr>,
    pub limit: Option<usize>,
}

pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}
```

### FieldValue

```rust
pub enum FieldValue {
    Integer(i64),
    Float(f64),
    String(Vec<u8>),
    Boolean(bool),
    Unsigned(u64),
}
```

## 错误类型

```rust
#[derive(Error, Debug)]
pub enum EngineError {
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),
    
    #[error("MemTable error: {0}")]
    MemTable(#[from] MemTableError),
    
    #[error("TSSP error: {0}")]
    Tssp(#[from] TsspError),
    
    #[error("Compression error: {0}")]
    Compression(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
```