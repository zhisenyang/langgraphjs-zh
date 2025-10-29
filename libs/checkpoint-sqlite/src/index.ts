// SQLite 检查点保存器实现
// 用于在 SQLite 数据库中持久化 LangGraph 的检查点数据
import Database, { Database as DatabaseType, Statement } from "better-sqlite3";
import type { RunnableConfig } from "@langchain/core/runnables";
import {
  BaseCheckpointSaver,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointTuple,
  type SerializerProtocol,
  type PendingWrite,
  type CheckpointMetadata,
  TASKS,
  copyCheckpoint,
  maxChannelVersion,
} from "@langchain/langgraph-checkpoint";

// 数据库检查点行的接口定义
interface CheckpointRow {
  checkpoint: string;           // 序列化的检查点数据
  metadata: string;            // 序列化的元数据
  parent_checkpoint_id?: string; // 父检查点ID
  thread_id: string;           // 线程ID
  checkpoint_id: string;       // 检查点ID
  checkpoint_ns?: string;      // 检查点命名空间
  type?: string;              // 序列化类型
  pending_writes: string;      // 待写入操作的JSON字符串
}

// 待写入操作的列结构
interface PendingWriteColumn {
  task_id: string;    // 任务ID
  channel: string;    // 通道名称
  type: string;       // 数据类型
  value: string;      // 序列化的值
}

// 待发送操作的列结构
interface PendingSendColumn {
  type: string;       // 数据类型
  value: string;      // 序列化的值
}

// 在 `SqliteSaver.list` 方法中，我们需要清理 `options.filter` 参数，确保它只包含
// `CheckpointMetadata` 类型中的键。下面的代码确保如果我们使用的键列表与
// `CheckpointMetadata` 类型不同步，会产生编译时错误。
const checkpointMetadataKeys = ["source", "step", "parents"] as const;

// 类型检查工具：确保键列表与类型定义保持同步
type CheckKeys<T, K extends readonly (keyof T)[]> = [K[number]] extends [
  keyof T
]
  ? [keyof T] extends [K[number]]
    ? K
    : never
  : never;

// 验证键列表的函数
function validateKeys<T, K extends readonly (keyof T)[]>(
  keys: CheckKeys<T, K>
): K {
  return keys;
}

// 如果这行代码编译失败，说明我们在 `SqliteSaver.list` 方法中使用的键列表与
// `CheckpointMetadata` 类型不同步。此时需要更新 `checkpointMetadataKeys` 以包含
// `CheckpointMetadata` 中的所有键
const validCheckpointMetadataKeys = validateKeys<
  CheckpointMetadata,
  typeof checkpointMetadataKeys
>(checkpointMetadataKeys);

// 准备SQL查询语句的函数
// 根据是否需要特定检查点ID来构建不同的查询
function prepareSql(db: DatabaseType, checkpointId: boolean) {
  const sql = `
  SELECT
    thread_id,
    checkpoint_ns,
    checkpoint_id,
    parent_checkpoint_id,
    type,
    checkpoint,
    metadata,
    (
      -- 子查询：获取当前检查点的所有待写入操作
      SELECT
        json_group_array(
          json_object(
            'task_id', pw.task_id,
            'channel', pw.channel,
            'type', pw.type,
            'value', CAST(pw.value AS TEXT)
          )
        )
      FROM writes as pw
      WHERE pw.thread_id = checkpoints.thread_id
        AND pw.checkpoint_ns = checkpoints.checkpoint_ns
        AND pw.checkpoint_id = checkpoints.checkpoint_id
    ) as pending_writes,
    (
      -- 子查询：获取父检查点的待发送任务
      SELECT
        json_group_array(
          json_object(
            'type', ps.type,
            'value', CAST(ps.value AS TEXT)
          )
        )
      FROM writes as ps
      WHERE ps.thread_id = checkpoints.thread_id
        AND ps.checkpoint_ns = checkpoints.checkpoint_ns
        AND ps.checkpoint_id = checkpoints.parent_checkpoint_id
        AND ps.channel = '${TASKS}'
      ORDER BY ps.idx
    ) as pending_sends
  FROM checkpoints
  WHERE thread_id = ? AND checkpoint_ns = ? ${
    checkpointId
      ? "AND checkpoint_id = ?"  // 查询特定检查点
      : "ORDER BY checkpoint_id DESC LIMIT 1"  // 查询最新检查点
  }`;

  return db.prepare(sql);
}

/**
 * SQLite 检查点保存器类
 * 继承自 BaseCheckpointSaver，实现在 SQLite 数据库中保存和检索检查点
 */
export class SqliteSaver extends BaseCheckpointSaver {
  db: DatabaseType;  // SQLite 数据库实例

  protected isSetup: boolean;  // 标记数据库是否已初始化

  protected withoutCheckpoint: Statement;  // 查询最新检查点的预编译语句

  protected withCheckpoint: Statement;     // 查询特定检查点的预编译语句

  /**
   * 构造函数
   * @param db SQLite 数据库实例
   * @param serde 序列化协议（可选）
   */
  constructor(db: DatabaseType, serde?: SerializerProtocol) {
    super(serde);
    this.db = db;
    this.isSetup = false;
  }

  /**
   * 从连接字符串或本地路径创建 SqliteSaver 实例
   * @param connStringOrLocalPath 数据库连接字符串或本地文件路径
   * @returns SqliteSaver 实例
   */
  static fromConnString(connStringOrLocalPath: string): SqliteSaver {
    return new SqliteSaver(new Database(connStringOrLocalPath));
  }

  /**
   * 初始化数据库表结构和预编译语句
   * 只在首次调用时执行，后续调用会直接返回
   */
  protected setup(): void {
    if (this.isSetup) {
      return;
    }

    // 启用 WAL 模式以提高并发性能
    this.db.pragma("journal_mode=WAL");
    
    // 创建检查点表：存储检查点的主要数据
    this.db.exec(`
CREATE TABLE IF NOT EXISTS checkpoints (
  thread_id TEXT NOT NULL,           -- 线程ID
  checkpoint_ns TEXT NOT NULL DEFAULT '',  -- 检查点命名空间
  checkpoint_id TEXT NOT NULL,       -- 检查点ID
  parent_checkpoint_id TEXT,         -- 父检查点ID
  type TEXT,                        -- 序列化类型
  checkpoint BLOB,                  -- 序列化的检查点数据
  metadata BLOB,                    -- 序列化的元数据
  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
);`);
    
    // 创建写入操作表：存储待执行的写入操作
    this.db.exec(`
CREATE TABLE IF NOT EXISTS writes (
  thread_id TEXT NOT NULL,           -- 线程ID
  checkpoint_ns TEXT NOT NULL DEFAULT '',  -- 检查点命名空间
  checkpoint_id TEXT NOT NULL,       -- 检查点ID
  task_id TEXT NOT NULL,            -- 任务ID
  idx INTEGER NOT NULL,             -- 操作索引（用于排序）
  channel TEXT NOT NULL,            -- 通道名称
  type TEXT,                       -- 数据类型
  value BLOB,                      -- 序列化的值
  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
);`);

    // 预编译SQL语句以提高查询性能
    this.withoutCheckpoint = prepareSql(this.db, false);  // 查询最新检查点
    this.withCheckpoint = prepareSql(this.db, true);      // 查询特定检查点

    this.isSetup = true;
  }

  /**
   * 获取检查点元组
   * 根据配置获取指定的检查点或最新的检查点
   * @param config 运行配置，包含线程ID、检查点命名空间和检查点ID
   * @returns 检查点元组或undefined（如果未找到）
   */
  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    this.setup();
    const {
      thread_id,
      checkpoint_ns = "",
      checkpoint_id,
    } = config.configurable ?? {};

    // 构建查询参数
    const args = [thread_id, checkpoint_ns];
    if (checkpoint_id) args.push(checkpoint_id);

    // 根据是否有检查点ID选择相应的预编译语句
    const stm = checkpoint_id ? this.withCheckpoint : this.withoutCheckpoint;
    const row = stm.get(...args) as CheckpointRow;
    if (row === undefined) return undefined;

    let finalConfig = config;

    // 如果没有指定检查点ID，使用查询到的最新检查点ID
    if (!checkpoint_id) {
      finalConfig = {
        configurable: {
          thread_id: row.thread_id,
          checkpoint_ns,
          checkpoint_id: row.checkpoint_id,
        },
      };
    }

    // 验证必需的配置参数
    if (
      finalConfig.configurable?.thread_id === undefined ||
      finalConfig.configurable?.checkpoint_id === undefined
    ) {
      throw new Error("Missing thread_id or checkpoint_id");
    }

    // 反序列化待写入操作
    const pendingWrites = await Promise.all(
      (JSON.parse(row.pending_writes) as PendingWriteColumn[]).map(
        async (write) => {
          return [
            write.task_id,
            write.channel,
            await this.serde.loadsTyped(
              write.type ?? "json",
              write.value ?? ""
            ),
          ] as [string, string, unknown];
        }
      )
    );

    // 反序列化检查点数据
    const checkpoint = (await this.serde.loadsTyped(
      row.type ?? "json",
      row.checkpoint
    )) as Checkpoint;

    // 对于版本小于4的检查点，需要迁移待发送操作
    if (checkpoint.v < 4 && row.parent_checkpoint_id != null) {
      await this.migratePendingSends(
        checkpoint,
        row.thread_id,
        row.parent_checkpoint_id
      );
    }

    return {
      checkpoint,
      config: finalConfig,
      metadata: (await this.serde.loadsTyped(
        row.type ?? "json",
        row.metadata
      )) as CheckpointMetadata,
      parentConfig: row.parent_checkpoint_id
        ? {
            configurable: {
              thread_id: row.thread_id,
              checkpoint_ns,
              checkpoint_id: row.parent_checkpoint_id,
            },
          }
        : undefined,
      pendingWrites,
    };
  }

  /**
   * 列出检查点
   * 根据配置和选项返回检查点列表的异步生成器
   * @param config 运行配置
   * @param options 列表选项（限制、过滤等）
   * @yields 检查点元组
   */
  async *list(
    config: RunnableConfig,
    options?: CheckpointListOptions
  ): AsyncGenerator<CheckpointTuple> {
    const { limit, before, filter } = options ?? {};
    this.setup();
    const thread_id = config.configurable?.thread_id;
    const checkpoint_ns = config.configurable?.checkpoint_ns;
    
    // 构建基础SQL查询
    let sql = `
      SELECT
        thread_id,
        checkpoint_ns,
        checkpoint_id,
        parent_checkpoint_id,
        type,
        checkpoint,
        metadata,
        (
          -- 获取当前检查点的待写入操作
          SELECT
            json_group_array(
              json_object(
                'task_id', pw.task_id,
                'channel', pw.channel,
                'type', pw.type,
                'value', CAST(pw.value AS TEXT)
              )
            )
          FROM writes as pw
          WHERE pw.thread_id = checkpoints.thread_id
            AND pw.checkpoint_ns = checkpoints.checkpoint_ns
            AND pw.checkpoint_id = checkpoints.checkpoint_id
        ) as pending_writes,
        (
          -- 获取父检查点的待发送任务
          SELECT
            json_group_array(
              json_object(
                'type', ps.type,
                'value', CAST(ps.value AS TEXT)
              )
            )
          FROM writes as ps
          WHERE ps.thread_id = checkpoints.thread_id
            AND ps.checkpoint_ns = checkpoints.checkpoint_ns
            AND ps.checkpoint_id = checkpoints.parent_checkpoint_id
            AND ps.channel = '${TASKS}'
          ORDER BY ps.idx
        ) as pending_sends
      FROM checkpoints\n`;

    // 构建WHERE子句
    const whereClause: string[] = [];

    if (thread_id) {
      whereClause.push("thread_id = ?");
    }

    if (checkpoint_ns !== undefined && checkpoint_ns !== null) {
      whereClause.push("checkpoint_ns = ?");
    }

    if (before?.configurable?.checkpoint_id !== undefined) {
      whereClause.push("checkpoint_id < ?");
    }

    // 清理和验证过滤器参数，只保留有效的元数据键
    const sanitizedFilter = Object.fromEntries(
      Object.entries(filter ?? {}).filter(
        ([key, value]) =>
          value !== undefined &&
          validCheckpointMetadataKeys.includes(key as keyof CheckpointMetadata)
      )
    );

    // 为过滤器添加JSON查询条件
    whereClause.push(
      ...Object.entries(sanitizedFilter).map(
        ([key]) => `jsonb(CAST(metadata AS TEXT))->'$.${key}' = ?`
      )
    );

    // 添加WHERE子句到SQL
    if (whereClause.length > 0) {
      sql += `WHERE\n  ${whereClause.join(" AND\n  ")}\n`;
    }

    // 按检查点ID降序排列
    sql += "\nORDER BY checkpoint_id DESC";

    // 添加限制条件
    if (limit) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      sql += ` LIMIT ${parseInt(limit as any, 10)}`; // 使用parseInt清理用户输入
    }

    // 构建查询参数数组
    const args = [
      thread_id,
      checkpoint_ns,
      before?.configurable?.checkpoint_id,
      ...Object.values(sanitizedFilter).map((value) => JSON.stringify(value)),
    ].filter((value) => value !== undefined && value !== null);

    // 执行查询
    const rows: CheckpointRow[] = this.db
      .prepare(sql)
      .all(...args) as CheckpointRow[];

    // 处理查询结果并生成检查点元组
    if (rows) {
      for (const row of rows) {
        // 反序列化待写入操作
        const pendingWrites = await Promise.all(
          (JSON.parse(row.pending_writes) as PendingWriteColumn[]).map(
            async (write) => {
              return [
                write.task_id,
                write.channel,
                await this.serde.loadsTyped(
                  write.type ?? "json",
                  write.value ?? ""
                ),
              ] as [string, string, unknown];
            }
          )
        );

        // 反序列化检查点数据
        const checkpoint = (await this.serde.loadsTyped(
          row.type ?? "json",
          row.checkpoint
        )) as Checkpoint;

        // 处理旧版本检查点的迁移
        if (checkpoint.v < 4 && row.parent_checkpoint_id != null) {
          await this.migratePendingSends(
            checkpoint,
            row.thread_id,
            row.parent_checkpoint_id
          );
        }

        // 生成检查点元组
        yield {
          config: {
            configurable: {
              thread_id: row.thread_id,
              checkpoint_ns: row.checkpoint_ns,
              checkpoint_id: row.checkpoint_id,
            },
          },
          checkpoint,
          metadata: (await this.serde.loadsTyped(
            row.type ?? "json",
            row.metadata
          )) as CheckpointMetadata,
          parentConfig: row.parent_checkpoint_id
            ? {
                configurable: {
                  thread_id: row.thread_id,
                  checkpoint_ns: row.checkpoint_ns,
                  checkpoint_id: row.parent_checkpoint_id,
                },
              }
            : undefined,
          pendingWrites,
        };
      }
    }
  }

  /**
   * 保存检查点
   * 将检查点和元数据序列化后存储到数据库中
   * @param config 运行配置
   * @param checkpoint 要保存的检查点
   * @param metadata 检查点元数据
   * @returns 更新后的运行配置
   */
  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata
  ): Promise<RunnableConfig> {
    this.setup();

    if (!config.configurable) {
      throw new Error("Empty configuration supplied.");
    }

    const thread_id = config.configurable?.thread_id;
    const checkpoint_ns = config.configurable?.checkpoint_ns ?? "";
    const parent_checkpoint_id = config.configurable?.checkpoint_id;

    if (!thread_id) {
      throw new Error(
        `Missing "thread_id" field in passed "config.configurable".`
      );
    }

    // 复制检查点以避免修改原始对象
    const preparedCheckpoint: Partial<Checkpoint> = copyCheckpoint(checkpoint);

    // 并行序列化检查点和元数据
    const [[type1, serializedCheckpoint], [type2, serializedMetadata]] =
      await Promise.all([
        this.serde.dumpsTyped(preparedCheckpoint),
        this.serde.dumpsTyped(metadata),
      ]);

    // 确保检查点和元数据使用相同的序列化类型
    if (type1 !== type2) {
      throw new Error(
        "Failed to serialized checkpoint and metadata to the same type."
      );
    }
    
    // 构建数据库行数据
    const row = [
      thread_id,
      checkpoint_ns,
      checkpoint.id,
      parent_checkpoint_id,
      type1,
      serializedCheckpoint,
      serializedMetadata,
    ];

    // 插入或替换检查点记录
    this.db
      .prepare(
        `INSERT OR REPLACE INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)`
      )
      .run(...row);

    return {
      configurable: {
        thread_id,
        checkpoint_ns,
        checkpoint_id: checkpoint.id,
      },
    };
  }

  /**
   * 保存待写入操作
   * 将待写入操作序列化后批量存储到数据库中
   * @param config 运行配置
   * @param writes 待写入操作数组
   * @param taskId 任务ID
   */
  async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string
  ): Promise<void> {
    this.setup();

    if (!config.configurable) {
      throw new Error("Empty configuration supplied.");
    }

    if (!config.configurable?.thread_id) {
      throw new Error("Missing thread_id field in config.configurable.");
    }

    if (!config.configurable?.checkpoint_id) {
      throw new Error("Missing checkpoint_id field in config.configurable.");
    }

    // 准备插入语句
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO writes 
      (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    // 创建事务以确保原子性操作
    const transaction = this.db.transaction((rows) => {
      for (const row of rows) {
        stmt.run(...row);
      }
    });

    // 序列化所有写入操作并构建行数据
    const rows = await Promise.all(
      writes.map(async (write, idx) => {
        const [type, serializedWrite] = await this.serde.dumpsTyped(write[1]);
        return [
          config.configurable?.thread_id,
          config.configurable?.checkpoint_ns,
          config.configurable?.checkpoint_id,
          taskId,
          idx,  // 索引用于保持操作顺序
          write[0],  // 通道名称
          type,
          serializedWrite,
        ];
      })
    );

    // 执行事务
    transaction(rows);
  }

  /**
   * 删除线程
   * 删除指定线程ID的所有检查点和写入操作记录
   * @param threadId 要删除的线程ID
   */
  async deleteThread(threadId: string) {
    // 使用事务确保删除操作的原子性
    const transaction = this.db.transaction(() => {
      // 删除检查点记录
      this.db
        .prepare(`DELETE FROM checkpoints WHERE thread_id = ?`)
        .run(threadId);
      // 删除写入操作记录
      this.db.prepare(`DELETE FROM writes WHERE thread_id = ?`).run(threadId);
    });

    transaction();
  }

  /**
   * 迁移待发送操作（用于版本兼容性）
   * 将旧版本检查点中的待发送操作迁移到新的格式
   * @param checkpoint 要迁移的检查点
   * @param threadId 线程ID
   * @param parentCheckpointId 父检查点ID
   */
  protected async migratePendingSends(
    checkpoint: Checkpoint,
    threadId: string,
    parentCheckpointId: string
  ) {
    // 查询父检查点中的待发送任务
    const { pending_sends } = this.db
      .prepare(
        `
          SELECT
            checkpoint_id,
            json_group_array(
              json_object(
                'type', ps.type,
                'value', CAST(ps.value AS TEXT)
              )
            ) as pending_sends
          FROM writes as ps
          WHERE ps.thread_id = ?
            AND ps.checkpoint_id = ?
            AND ps.channel = '${TASKS}'
          ORDER BY ps.idx
        `
      )
      .get(threadId, parentCheckpointId) as { pending_sends: string };

    const mutableCheckpoint = checkpoint;

    // 将待发送操作添加到检查点的通道值中
    mutableCheckpoint.channel_values ??= {};
    mutableCheckpoint.channel_values[TASKS] = await Promise.all(
      JSON.parse(pending_sends).map(({ type, value }: PendingSendColumn) =>
        this.serde.loadsTyped(type, value)
      )
    );

    // 更新通道版本号
    mutableCheckpoint.channel_versions[TASKS] =
      Object.keys(checkpoint.channel_versions).length > 0
        ? maxChannelVersion(...Object.values(checkpoint.channel_versions))
        : this.getNextVersion(undefined);
  }
}
