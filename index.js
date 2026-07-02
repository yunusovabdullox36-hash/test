// 🏠 Agent Room Server v3
// Express + In-Memory Store (MongoDB optional) — Agent Registry, Task Queue, Skill Routing, WebSocket
require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json());

// ─── In-Memory Store ─────────────────────────────────────────
// MongoDB is OPTIONAL. This store works without any DB.
class MemoryStore {
  constructor() {
    this.agents = new Map();
    this.tasks = new Map();
    this.logs = [];
    this.maxLogs = 500;
  }

  // Agents
  async findAgent(filter) {
    const agents = Array.from(this.agents.values());
    return agents.filter(a => {
      for (const [k, v] of Object.entries(filter)) {
        if (k === '$or') continue;
        if (a[k] !== v) return false;
      }
      return true;
    });
  }

  async findOneAgent(filter) {
    for (const a of this.agents.values()) {
      let match = true;
      for (const [k, v] of Object.entries(filter)) {
        if (a[k] !== v) { match = false; break; }
      }
      if (match) return a;
    }
    return null;
  }

  async upsertAgent(agentId, data) {
    const existing = this.agents.get(agentId) || { agentId };
    const updated = { ...existing, ...data, lastHeartbeat: new Date() };
    this.agents.set(agentId, updated);
    return updated;
  }

  async updateAgent(agentId, data) {
    const agent = this.agents.get(agentId);
    if (!agent) return null;
    Object.assign(agent, data);
    return agent;
  }

  async countAgents(filter = {}) {
    let count = 0;
    for (const a of this.agents.values()) {
      let match = true;
      for (const [k, v] of Object.entries(filter)) {
        if (a[k] !== v) { match = false; break; }
      }
      if (match) count++;
    }
    return count;
  }

  // Tasks
  async createTask(data) {
    const task = { ...data, createdAt: new Date(), updatedAt: new Date() };
    this.tasks.set(task.taskId, task);
    return task;
  }

  async findTasks(filter) {
    const tasks = Array.from(this.tasks.values());
    return tasks.filter(t => {
      for (const [k, v] of Object.entries(filter)) {
        if (t[k] !== v) return false;
      }
      return true;
    }).sort((a, b) => (b.priority || 0) - (a.priority || 0));
  }

  async findOneTask(taskId) {
    return this.tasks.get(taskId) || null;
  }

  async updateTask(taskId, data) {
    const task = this.tasks.get(taskId);
    if (!task) return null;
    Object.assign(task, data, { updatedAt: new Date() });
    return task;
  }

  async countTasks(filter = {}) {
    let count = 0;
    for (const t of this.tasks.values()) {
      let match = true;
      for (const [k, v] of Object.entries(filter)) {
        if (t[k] !== v) { match = false; break; }
      }
      if (match) count++;
    }
    return count;
  }

  // Logs
  async addLog(entry) {
    const log = { ...entry, createdAt: new Date() };
    this.logs.unshift(log);
    if (this.logs.length > this.maxLogs) this.logs.pop();
    return log;
  }

  async findLogs(filter, limit = 100) {
    let logs = this.logs;
    for (const [k, v] of Object.entries(filter)) {
      logs = logs.filter(l => l[k] === v);
    }
    return logs.slice(0, limit);
  }
}

// ─── Store: Start with memory, try MongoDB upgrade in background ───
let store = new MemoryStore();
initDefaultAgents(); // Works with MemoryStore immediately

// Try MongoDB in background (non-blocking)
setTimeout(async () => {
  try {
    const mongoose = require('mongoose');
    const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/agent-room';
    const agentSchema = new mongoose.Schema({ agentId: { type: String, unique: true, required: true }, name: String, type: { type: String, enum: ['opencode', 'codebuff', 'custom', 'cli', 'ai-agent'], default: 'custom' }, status: { type: String, enum: ['online', 'offline', 'busy'], default: 'offline' }, skills: [String], currentTask: { type: String, default: null }, taskHistory: [{ taskId: String, completedAt: Date }], lastHeartbeat: { type: Date, default: Date.now }, metadata: { type: Object, default: {} } }, { timestamps: true });
    const taskSchema = new mongoose.Schema({ taskId: { type: String, unique: true, required: true }, title: String, description: String, project: String, agentId: { type: String, default: null }, requiredSkills: [String], type: { type: String, default: 'general' }, payload: { type: Object, default: {} }, status: { type: String, enum: ['queued', 'assigned', 'running', 'done', 'failed', 'cancelled'], default: 'queued' }, result: { type: Object, default: null }, priority: { type: Number, default: 0 }, createdBy: String, assignedAt: Date, completedAt: Date }, { timestamps: true });
    const logSchema = new mongoose.Schema({ type: String, agentId: String, taskId: String, message: String, metadata: { type: Object, default: {} } }, { timestamps: true });
    const Agent = mongoose.model('Agent', agentSchema);
    const Task = mongoose.model('Task', taskSchema);
    const RoomLog = mongoose.model('RoomLog', logSchema);
    await mongoose.connect(MONGODB_URI, { serverSelectionTimeoutMS: 5000, connectTimeoutMS: 5000 });
    console.log(`✅ MongoDB connected: ${MONGODB_URI}`);
    // Upgrade store to MongoDB
    const mongoStore = {};
    mongoStore.findAgent = (filter) => Agent.find(filter).sort({ status: 1, name: 1 });
    mongoStore.findOneAgent = (filter) => Agent.findOne(filter);
    mongoStore.upsertAgent = (agentId, data) => Agent.findOneAndUpdate({ agentId }, { ...data, lastHeartbeat: new Date() }, { upsert: true, returnDocument: 'after' });
    mongoStore.updateAgent = (agentId, data) => Agent.findOneAndUpdate({ agentId }, data, { returnDocument: 'after' });
    mongoStore.countAgents = (filter = {}) => Agent.countDocuments(filter);
    mongoStore.createTask = (data) => Task.create(data);
    mongoStore.findTasks = (filter) => Task.find(filter).sort({ priority: -1, createdAt: -1 });
    mongoStore.findOneTask = (taskId) => Task.findOne({ taskId });
    mongoStore.updateTask = (taskId, data) => Task.findOneAndUpdate({ taskId }, data, { returnDocument: 'after' });
    mongoStore.countTasks = (filter = {}) => Task.countDocuments(filter);
    mongoStore.addLog = (entry) => RoomLog.create(entry);
    mongoStore.findLogs = (filter, lim = 100) => RoomLog.find(filter).sort({ createdAt: -1 }).limit(lim);
    store = mongoStore;
    await initDefaultAgents(); // Re-register agents in MongoDB
  } catch (err) {
    if (err.code !== 'MODULE_NOT_FOUND') console.error('⚠️ MongoDB unavailable, staying on memory store:', err.message);
    else console.log('⚠️ MongoDB package not installed, staying on memory store');
  }
}, 100); // 100ms delay so server starts first

// ─── Helper: Skill-based task routing ───────────────────────

async function autoAssignTask(task) {
  const availableAgents = await store.findAgent({ status: 'online' });
  const filtered = [];
  for (const agent of availableAgents) {
    if (!agent.currentTask) filtered.push(agent);
  }

  if (filtered.length === 0) {
    console.log(`⚠️ No available agents for task "${task.title}"`);
    return null;
  }

  const scored = filtered.map(agent => {
    let score = 0;
    if (task.requiredSkills && task.requiredSkills.length > 0) {
      const matchedSkills = task.requiredSkills.filter(s =>
        (agent.skills || []).some(as => as.toLowerCase().includes(s.toLowerCase()))
      );
      score = matchedSkills.length / task.requiredSkills.length;
    } else {
      score = 0.5;
    }
    return { agent, score };
  });

  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];

  if (best.score === 0 && task.requiredSkills?.length > 0) {
    console.log(`⚠️ No matching skills for task "${task.title}" — assigning to ${best.agent.name}`);
  }

  best.agent.currentTask = task.taskId;
  best.agent.status = 'busy';
  await store.upsertAgent(best.agent.agentId, best.agent);

  task.agentId = best.agent.agentId;
  task.status = 'assigned';
  task.assignedAt = new Date();
  await store.updateTask(task.taskId, task);

  await store.addLog({
    type: 'task', agentId: best.agent.agentId, taskId: task.taskId,
    message: `Task "${task.title}" assigned to ${best.agent.name} (skill match: ${Math.round(best.score * 100)}%)`
  });

  broadcast({ type: 'task:assigned', task, agent: best.agent });
  return best.agent;
}

// ─── Register default agents ────────────────────────────────

async function initDefaultAgents() {
  const defaultAgents = [
    { agentId: 'room', name: 'ROOM (OpenCode)', type: 'opencode', skills: ['general', 'fullstack', 'orchestration', 'express', 'mongodb', 'react', 'nextjs'] },
    { agentId: 'codebuff', name: 'Codebuff', type: 'codebuff', skills: ['general', 'strategy', 'orchestration', 'project-management'] },
    { agentId: 'telegram-bot', name: '@telegram-bot', type: 'custom', skills: ['communication', 'messaging', 'telegram'] },
    { agentId: 'backend-agent', name: '@backend', type: 'ai-agent', skills: ['backend', 'express', 'mongodb', 'jwt', 'api', 'auth', 'rest', 'crud'] },
    { agentId: 'frontend-agent', name: '@frontend', type: 'ai-agent', skills: ['frontend', 'react', 'nextjs', 'tailwind', 'ui', 'shadcn', 'typescript'] },
    { agentId: 'reviewer-agent', name: '@code-reviewer', type: 'ai-agent', skills: ['review', 'quality', 'best-practices', 'code-analysis', 'debugging'] },
    { agentId: 'security-agent', name: '@security-auditor', type: 'ai-agent', skills: ['security', 'owasp', 'audit', 'pentest', 'compliance'] },
    { agentId: 'docs-agent', name: '@docs-writer', type: 'ai-agent', skills: ['documentation', 'prd', 'adr', 'writing', 'technical-writing'] },
    { agentId: 'game-agent', name: '@game-dev', type: 'ai-agent', skills: ['game-dev', 'threejs', 'phaser', 'canvas', 'webgl', 'animation'] },
    { agentId: 'intake-agent', name: '@intake', type: 'ai-agent', skills: ['research', 'analysis', 'context-gathering', 'information-retrieval'] },
    { agentId: 'critic-agent', name: '@critic', type: 'ai-agent', skills: ['review', 'critique', 'analysis', 'alternative-approaches'] },
    { agentId: 'handoff-guard', name: '@handoff-guard', type: 'custom', skills: ['monitoring', 'context-tracking', 'session-management'] },
    { agentId: 'code-searcher', name: 'code-searcher', type: 'cli', skills: ['search', 'grep', 'code-search', 'pattern-matching'] },
    { agentId: 'file-picker', name: 'file-picker', type: 'cli', skills: ['file-search', 'fuzzy-find', 'file-discovery'] },
    { agentId: 'researcher-web', name: 'researcher-web', type: 'cli', skills: ['web-research', 'information-retrieval', 'scraping'] },
    { agentId: 'researcher-docs', name: 'researcher-docs', type: 'cli', skills: ['documentation', 'docs-research', 'api-docs'] },
    { agentId: 'thinker-gemini', name: 'thinker-with-files-gemini', type: 'cli', skills: ['deep-thinking', 'analysis', 'complex-problems', 'reasoning'] },
    { agentId: 'basher', name: 'basher', type: 'cli', skills: ['terminal', 'shell', 'commands', 'automation'] },
    { agentId: 'code-reviewer-deepseek', name: 'code-reviewer-deepseek', type: 'cli', skills: ['deep-review', 'code-analysis', 'quality'] },
    { agentId: 'vault-filler', name: 'vault-filler', type: 'custom', skills: ['obsidian', 'note-filling', 'vault-maintenance', 'research'] },
    { agentId: 'explore', name: 'explore', type: 'opencode', skills: ['search', 'exploration', 'file-discovery', 'codebase-navigation'] },
    { agentId: 'general', name: 'general', type: 'opencode', skills: ['general', 'multi-purpose', 'any-task'] }
  ];

  for (const agent of defaultAgents) {
    await store.upsertAgent(agent.agentId, { ...agent, status: 'online', lastHeartbeat: new Date() });
  }
  console.log(`✅ ${defaultAgents.length} default agents registered`);
}

// ─── Agent Registry API ─────────────────────────────────────

app.post('/api/agents/register', async (req, res) => {
  try {
    const { agentId, name, type, skills, metadata } = req.body;
    const agent = await store.upsertAgent(agentId, { agentId, name, type, skills: skills || [], status: 'online', metadata });
    await store.addLog({ type: 'agent', agentId, message: `${name} joined the Room` });
    res.json(agent);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/agents', async (req, res) => {
  try {
    const filter = {};
    if (req.query.status) filter.status = req.query.status;
    let agents = await store.findAgent(filter);
    if (req.query.skill) {
      const skill = req.query.skill.toLowerCase();
      agents = agents.filter(a => (a.skills || []).some(s => s.toLowerCase().includes(skill)));
    }
    res.json(agents);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/agents/:id', async (req, res) => {
  try {
    const agent = await store.findOneAgent({ agentId: req.params.id });
    if (!agent) return res.status(404).json({ error: 'Agent not found' });
    res.json(agent);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/agents/:id/heartbeat', async (req, res) => {
  try {
    const agent = await store.upsertAgent(req.params.id, { status: 'online' });
    if (!agent) return res.status(404).json({ error: 'Agent not found' });
    res.json(agent);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Task API ────────────────────────────────────────────────

app.post('/api/tasks', async (req, res) => {
  try {
    const { title, description, project, agentId, requiredSkills, priority, createdBy, type, payload } = req.body;
    const task = await store.createTask({
      taskId: `task-${uuidv4().slice(0, 8)}`,
      title, description, project, agentId, requiredSkills: requiredSkills || [],
      priority: priority || 0, createdBy, type: type || 'general', payload,
      status: 'queued'
    });

    await store.addLog({ type: 'task', taskId: task.taskId, message: `Task created: "${title}" [${type}]` });

    if (!agentId) {
      const assigned = await autoAssignTask(task);
      if (assigned) return res.json({ task, assigned: true, agent: assigned });
    }

    broadcast({ type: 'task:new', task });
    res.json({ task, assigned: false });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/tasks', async (req, res) => {
  try {
    const filter = {};
    if (req.query.status) filter.status = req.query.status;
    if (req.query.agentId) filter.agentId = req.query.agentId;
    if (req.query.project) filter.project = req.query.project;
    const tasks = await store.findTasks(filter);
    res.json(tasks);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/tasks/:id', async (req, res) => {
  try {
    const { status, result } = req.body;
    const update = {};
    if (status) update.status = status;
    if (result) update.result = result;
    if (status === 'done' || status === 'failed') update.completedAt = new Date();

    const task = await store.findOneTask(req.params.id);
    if (!task) return res.status(404).json({ error: 'Task not found' });

    if (status) task.status = status;
    if (result) task.result = result;
    if (status === 'done' || status === 'failed') task.completedAt = new Date();
    await store.updateTask(req.params.id, task);

    if (status === 'done' || status === 'failed' || status === 'cancelled') {
      await store.updateAgent(task.agentId, { status: 'online', currentTask: null });
    }

    await store.addLog({ type: 'task', agentId: task.agentId, taskId: task.taskId, message: `Task "${task.title}" → ${status}` });
    broadcast({ type: 'task:status', task });
    res.json(task);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/tasks/:id/reassign', async (req, res) => {
  try {
    const { agentId } = req.body;
    if (!agentId) return res.status(400).json({ error: 'agentId required' });

    const task = await store.findOneTask(req.params.id);
    if (!task) return res.status(404).json({ error: 'Task not found' });

    if (task.agentId) {
      await store.updateAgent(task.agentId, { status: 'online', currentTask: null });
    }

    const agent = await store.findOneAgent({ agentId });
    if (!agent) return res.status(404).json({ error: 'Agent not found' });

    agent.currentTask = task.taskId;
    agent.status = 'busy';
    await store.upsertAgent(agentId, agent);

    task.agentId = agentId;
    task.status = 'assigned';
    task.assignedAt = new Date();
    await store.updateTask(task.taskId, task);

    res.json({ task, agent });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Room Log API ───────────────────────────────────────────

app.get('/api/logs', async (req, res) => {
  try {
    const filter = {};
    if (req.query.type) filter.type = req.query.type;
    if (req.query.agentId) filter.agentId = req.query.agentId;
    const logs = await store.findLogs(filter);
    res.json(logs);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Stats Dashboard API ────────────────────────────────────

app.get('/api/stats', async (req, res) => {
  try {
    const [totalAgents, onlineAgents, busyAgents, totalTasks, pendingTasks, doneTasks] = await Promise.all([
      store.countAgents(),
      store.countAgents({ status: 'online' }),
      store.countAgents({ status: 'busy' }),
      store.countTasks(),
      store.countTasks({ status: { $in: ['queued', 'assigned'] } }),
      store.countTasks({ status: 'done' })
    ]);
    const storeType = store instanceof MemoryStore ? 'memory' : 'mongodb';
    res.json({
      status: 'healthy',
      store: storeType,
      agents: { total: totalAgents, online: onlineAgents, busy: busyAgents },
      tasks: { total: totalTasks, pending: pendingTasks, done: doneTasks }
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Root health check ──────────────────────────────────────

app.get('/', (req, res) => {
  res.json({
    server: '🏠 Agent Room Server',
    version: 'v3',
    status: 'healthy',
    endpoints: ['/api/agents', '/api/tasks', '/api/logs', '/api/stats', '/ws']
  });
});

// ─── WebSocket Server ───────────────────────────────────────

const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const clients = new Map();

function broadcast(data, excludeAgentId = null) {
  const msg = JSON.stringify(data);
  clients.forEach((ws, agentId) => {
    if (agentId !== excludeAgentId && ws.readyState === 1) {
      ws.send(msg);
    }
  });
}

wss.on('connection', (ws) => {
  let connectedAgentId = null;

  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw);
      const { type, from, to, payload } = msg;

      if (type === 'agent:connect') {
        connectedAgentId = from;
        clients.set(from, ws);
        await store.upsertAgent(from, { status: 'online' });
        broadcast({ type: 'agent:online', agentId: from });
        await store.addLog({ type: 'agent', agentId: from, message: 'Connected via WebSocket' });
        return;
      }

      if (type === 'agent:heartbeat') {
        await store.upsertAgent(from, { status: 'online' });
        return;
      }

      if (type === 'agent:message' && to) {
        const targetWs = clients.get(to);
        if (targetWs && targetWs.readyState === 1) targetWs.send(raw.toString());
        return;
      }

      if (type === 'agent:broadcast') {
        broadcast(raw, from);
        return;
      }

      if (type === 'task:status' || type === 'task:assign') {
        broadcast(raw, from);
        if (payload?.taskId) {
          const update = {};
          if (payload.status) update.status = payload.status;
          if (payload.status === 'done' || payload.status === 'failed') update.completedAt = new Date();
          await store.updateTask(payload.taskId, payload);
        }
        return;
      }

      if (type === 'obsidian:log') {
        await store.addLog({ type: 'memory', agentId: from, message: payload?.message || 'Obsidian log entry', metadata: payload });
        return;
      }
    } catch (err) {
      console.error('WS message error:', err.message);
    }
  });

  ws.on('close', async () => {
    if (connectedAgentId) {
      clients.delete(connectedAgentId);
      await store.upsertAgent(connectedAgentId, { status: 'offline' });
      broadcast({ type: 'agent:offline', agentId: connectedAgentId });
    }
  });
});

// ─── Heartbeat Monitor ──────────────────────────────────────

setInterval(async () => {
  const timeout = Date.now() - 45000;
  const allAgents = await store.findAgent({});
  for (const agent of allAgents) {
    if ((agent.status === 'online' || agent.status === 'busy') &&
        new Date(agent.lastHeartbeat).getTime() < timeout) {
      await store.upsertAgent(agent.agentId, { status: 'offline', currentTask: null });
      broadcast({ type: 'agent:offline', agentId: agent.agentId });
    }
  }
}, 15000);

// ─── Periodic task reassignment for stuck tasks ─────────────

setInterval(async () => {
  const allTasks = await store.findTasks({ status: 'assigned' });
  for (const task of allTasks) {
    if (task.assignedAt && new Date(task.assignedAt).getTime() < Date.now() - 5 * 60 * 1000) {
      console.log(`⚠️ Task "${task.title}" stuck — reassigning...`);
      await store.updateAgent(task.agentId, { status: 'online', currentTask: null });
      task.status = 'queued';
      task.agentId = null;
      task.assignedAt = null;
      await store.updateTask(task.taskId, task);
      await autoAssignTask(task);
    }
  }
}, 60000);

// ─── Start Server ───────────────────────────────────────────

const PORT = process.env.PORT || 4000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🏠 Agent Room Server v3 running on http://0.0.0.0:${PORT}`);
  console.log(`🔌 WebSocket ready on ws://0.0.0.0:${PORT}`);
  console.log(`📊 Stats: http://0.0.0.0:${PORT}/api/stats`);
});
