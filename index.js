// 🏠 Agent Room Server v2
// Express + MongoDB + WebSocket — Agent Registry, Task Queue, Skill Routing, Obsidian Logging

require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json());

// ─── MongoDB Models ──────────────────────────────────────────

const agentSchema = new mongoose.Schema({
  agentId: { type: String, unique: true, required: true },
  name: String,
  type: { type: String, enum: ['opencode', 'codebuff', 'custom', 'cli', 'ai-agent'], default: 'custom' },
  status: { type: String, enum: ['online', 'offline', 'busy'], default: 'offline' },
  skills: [String],
  currentTask: { type: String, default: null },
  taskHistory: [{ taskId: String, completedAt: Date }],
  lastHeartbeat: { type: Date, default: Date.now },
  metadata: { type: Object, default: {} }
}, { timestamps: true });

const taskSchema = new mongoose.Schema({
  taskId: { type: String, unique: true, required: true },
  title: String,
  description: String,
  project: String,
  agentId: { type: String, default: null },
  requiredSkills: [String],
  type: { type: String, default: 'general' },
  payload: { type: Object, default: {} },
  status: {
    type: String,
    enum: ['queued', 'assigned', 'running', 'done', 'failed', 'cancelled'],
    default: 'queued'
  },
  result: { type: Object, default: null },
  priority: { type: Number, default: 0, min: 0, max: 10 },
  createdBy: String,
  assignedAt: Date,
  completedAt: Date
}, { timestamps: true });

const logSchema = new mongoose.Schema({
  type: String, // 'task', 'agent', 'system', 'memory', 'obsidian'
  agentId: String,
  taskId: String,
  message: String,
  metadata: { type: Object, default: {} }
}, { timestamps: true });

const Agent = mongoose.model('Agent', agentSchema);
const Task = mongoose.model('Task', taskSchema);
const RoomLog = mongoose.model('RoomLog', logSchema);

// ─── Helper: Skill-based task routing ───────────────────────

async function autoAssignTask(task) {
  // Find best agent for this task based on skills and availability
  const availableAgents = await Agent.find({
    status: 'online',
    $or: [
      { currentTask: null },
      { currentTask: { $exists: false } }
    ]
  });

  if (availableAgents.length === 0) {
    console.log(`⚠️ No available agents for task "${task.title}"`);
    return null;
  }

  // Score agents by skill match
  const scored = availableAgents.map(agent => {
    let score = 0;
    if (task.requiredSkills && task.requiredSkills.length > 0) {
      const matchedSkills = task.requiredSkills.filter(s => 
        agent.skills.some(as => as.toLowerCase().includes(s.toLowerCase()))
      );
      score = matchedSkills.length / task.requiredSkills.length;
    } else {
      score = 0.5; // Default score if no skills required
    }
    return { agent, score };
  });

  // Pick highest scoring agent (or random if tie)
  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];

  if (best.score === 0 && task.requiredSkills?.length > 0) {
    console.log(`⚠️ No matching skills for task "${task.title}" — assigning to ${best.agent.name}`);
  }

  // Assign
  best.agent.currentTask = task.taskId;
  best.agent.status = 'busy';
  await best.agent.save();

  task.agentId = best.agent.agentId;
  task.status = 'assigned';
  task.assignedAt = new Date();
  await task.save();

  await RoomLog.create({
    type: 'task',
    agentId: best.agent.agentId,
    taskId: task.taskId,
    message: `Task "${task.title}" assigned to ${best.agent.name} (skill match: ${Math.round(best.score * 100)}%)`
  });

  broadcast({ type: 'task:assigned', task: task.toObject(), agent: best.agent.toObject() });
  return best.agent;
}

// ─── Agent Registry API ─────────────────────────────────────

app.post('/api/agents/register', async (req, res) => {
  try {
    const { agentId, name, type, skills, metadata } = req.body;
    const agent = await Agent.findOneAndUpdate(
      { agentId },
      { agentId, name, type, skills: skills || [], status: 'online', lastHeartbeat: new Date(), metadata },
      { upsert: true, new: true }
    );
    await RoomLog.create({ type: 'agent', agentId, message: `${name} joined the Room` });
    res.json(agent);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/agents', async (req, res) => {
  try {
    const filter = {};
    if (req.query.status) filter.status = req.query.status;
    if (req.query.skill) filter.skills = { $in: [req.query.skill] };
    const agents = await Agent.find(filter).sort({ status: 1, name: 1 });
    res.json(agents);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/agents/:id', async (req, res) => {
  try {
    const agent = await Agent.findOne({ agentId: req.params.id });
    if (!agent) return res.status(404).json({ error: 'Agent not found' });
    res.json(agent);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/agents/:id/heartbeat', async (req, res) => {
  try {
    const agent = await Agent.findOneAndUpdate(
      { agentId: req.params.id },
      { lastHeartbeat: new Date(), status: 'online' },
      { new: true }
    );
    if (!agent) return res.status(404).json({ error: 'Agent not found' });
    res.json(agent);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Task API ────────────────────────────────────────────────

app.post('/api/tasks', async (req, res) => {
  try {
    const { title, description, project, agentId, requiredSkills, priority, createdBy, type, payload } = req.body;
    const task = await Task.create({
      taskId: `task-${uuidv4().slice(0, 8)}`,
      title, description, project, agentId, requiredSkills: requiredSkills || [],
      priority: priority || 0, createdBy, type: type || 'general', payload
    });

    await RoomLog.create({
      type: 'task',
      taskId: task.taskId,
      message: `Task created: "${title}" [${type}]`
    });

    // Auto-assign if no specific agent requested
    if (!agentId) {
      const assigned = await autoAssignTask(task);
      if (assigned) {
        return res.json({ task: task.toObject(), assigned: true, agent: assigned.toObject() });
      }
    }

    broadcast({ type: 'task:new', task: task.toObject() });
    res.json({ task: task.toObject(), assigned: false });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/tasks', async (req, res) => {
  try {
    const filter = {};
    if (req.query.status) filter.status = req.query.status;
    if (req.query.agentId) filter.agentId = req.query.agentId;
    if (req.query.project) filter.project = req.query.project;
    const tasks = await Task.find(filter).sort({ priority: -1, createdAt: -1 });
    res.json(tasks);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/tasks/:id', async (req, res) => {
  try {
    const { status, result, agentId } = req.body;
    const update = {};
    if (status) update.status = status;
    if (result) update.result = result;
    if (agentId) update.agentId = agentId;
    if (status === 'done' || status === 'failed') update.completedAt = new Date();

    const task = await Task.findOneAndUpdate(
      { taskId: req.params.id },
      update,
      { new: true }
    );
    if (!task) return res.status(404).json({ error: 'Task not found' });

    // If done/failed, free the agent
    if (status === 'done' || status === 'failed' || status === 'cancelled') {
      await Agent.findOneAndUpdate(
        { agentId: task.agentId },
        { status: 'online', currentTask: null,
          $push: { taskHistory: { taskId: task.taskId, completedAt: new Date() } }
        }
      );
    }

    await RoomLog.create({
      type: 'task', agentId: task.agentId, taskId: task.taskId,
      message: `Task "${task.title}" → ${status}`
    });

    broadcast({ type: 'task:status', task: task.toObject() });
    res.json(task);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Re-assign task manually
app.post('/api/tasks/:id/reassign', async (req, res) => {
  try {
    const { agentId } = req.body;
    if (!agentId) return res.status(400).json({ error: 'agentId required' });

    const task = await Task.findOne({ taskId: req.params.id });
    if (!task) return res.status(404).json({ error: 'Task not found' });

    // Free previous agent
    if (task.agentId) {
      await Agent.findOneAndUpdate(
        { agentId: task.agentId },
        { status: 'online', currentTask: null }
      );
    }

    // Assign new agent
    const agent = await Agent.findOne({ agentId });
    if (!agent) return res.status(404).json({ error: 'Agent not found' });

    agent.currentTask = task.taskId;
    agent.status = 'busy';
    await agent.save();

    task.agentId = agentId;
    task.status = 'assigned';
    task.assignedAt = new Date();
    await task.save();

    res.json({ task: task.toObject(), agent: agent.toObject() });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Room Log API ───────────────────────────────────────────

app.get('/api/logs', async (req, res) => {
  try {
    const filter = {};
    if (req.query.type) filter.type = req.query.type;
    if (req.query.agentId) filter.agentId = req.query.agentId;
    const logs = await RoomLog.find(filter).sort({ createdAt: -1 }).limit(100);
    res.json(logs);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Stats Dashboard API ────────────────────────────────────

app.get('/api/stats', async (req, res) => {
  try {
    const [totalAgents, onlineAgents, busyAgents, totalTasks, pendingTasks, doneTasks] = await Promise.all([
      Agent.countDocuments(),
      Agent.countDocuments({ status: 'online' }),
      Agent.countDocuments({ status: 'busy' }),
      Task.countDocuments(),
      Task.countDocuments({ status: { $in: ['queued', 'assigned'] } }),
      Task.countDocuments({ status: 'done' })
    ]);
    res.json({
      agents: { total: totalAgents, online: onlineAgents, busy: busyAgents },
      tasks: { total: totalTasks, pending: pendingTasks, done: doneTasks }
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── WebSocket Server ───────────────────────────────────────

const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const clients = new Map(); // agentId → ws

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
        await Agent.findOneAndUpdate(
          { agentId: from },
          { status: 'online', lastHeartbeat: new Date() },
          { upsert: true }
        );
        broadcast({ type: 'agent:online', agentId: from });
        await RoomLog.create({ type: 'agent', agentId: from, message: 'Connected via WebSocket' });
        return;
      }

      if (type === 'agent:heartbeat') {
        await Agent.findOneAndUpdate(
          { agentId: from },
          { lastHeartbeat: new Date() }
        );
        return;
      }

      if (type === 'agent:message' && to) {
        const targetWs = clients.get(to);
        if (targetWs && targetWs.readyState === 1) {
          targetWs.send(raw.toString());
        }
        return;
      }

      if (type === 'agent:broadcast') {
        broadcast(raw, from);
        return;
      }

      if (type === 'task:status' || type === 'task:assign') {
        broadcast(raw, from);
        if (payload?.taskId) {
          const update = { status: payload.status, result: payload.result };
          if (payload.status === 'done' || payload.status === 'failed') update.completedAt = new Date();
          await Task.findOneAndUpdate({ taskId: payload.taskId }, update);
        }
        return;
      }

      if (type === 'obsidian:log') {
        await RoomLog.create({
          type: 'memory',
          agentId: from,
          message: payload?.message || 'Obsidian log entry',
          metadata: payload
        });
        return;
      }

    } catch (err) {
      console.error('WS message error:', err.message);
    }
  });

  ws.on('close', async () => {
    if (connectedAgentId) {
      clients.delete(connectedAgentId);
      await Agent.findOneAndUpdate(
        { agentId: connectedAgentId },
        { status: 'offline' }
      );
      broadcast({ type: 'agent:offline', agentId: connectedAgentId });
    }
  });
});

// ─── Heartbeat Monitor ──────────────────────────────────────

setInterval(async () => {
  const timeout = Date.now() - 45000; // 45 seconds
  const offlineAgents = await Agent.find({
    status: { $in: ['online', 'busy'] },
    lastHeartbeat: { $lt: new Date(timeout) }
  });
  for (const agent of offlineAgents) {
    agent.status = 'offline';
    agent.currentTask = null;
    await agent.save();
    broadcast({ type: 'agent:offline', agentId: agent.agentId });
  }
}, 15000);

// ─── Periodic task reassignment for stuck tasks ─────────────

setInterval(async () => {
  const stuckTasks = await Task.find({
    status: 'assigned',
    assignedAt: { $lt: new Date(Date.now() - 5 * 60 * 1000) } // 5 min timeout
  });
  for (const task of stuckTasks) {
    console.log(`⚠️ Task "${task.title}" stuck — reassigning...`);
    // Free the agent
    await Agent.findOneAndUpdate(
      { agentId: task.agentId },
      { status: 'online', currentTask: null }
    );
    task.status = 'queued';
    task.agentId = null;
    task.assignedAt = null;
    await task.save();
    // Try reassign
    await autoAssignTask(task);
  }
}, 60000);

// ─── Start Server ───────────────────────────────────────────

const PORT = process.env.PORT || 4000;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/agent-room';

mongoose.connect(MONGODB_URI)
  .then(async () => {
    console.log(`✅ MongoDB connected: ${MONGODB_URI}`);
    
    // Register ALL agents and sub-agents on startup
    const defaultAgents = [
      // 🏠 ROOM
      { agentId: 'room', name: 'ROOM (OpenCode)', type: 'opencode', skills: ['general', 'fullstack', 'orchestration', 'express', 'mongodb', 'react', 'nextjs'] },
      { agentId: 'codebuff', name: 'Codebuff', type: 'codebuff', skills: ['general', 'strategy', 'orchestration', 'project-management'] },
      { agentId: 'telegram-bot', name: '@telegram-bot', type: 'custom', skills: ['communication', 'messaging', 'telegram'] },
      
      // 🎯 MAIN AGENTS
      { agentId: 'backend-agent', name: '@backend', type: 'ai-agent', skills: ['backend', 'express', 'mongodb', 'jwt', 'api', 'auth', 'rest', 'crud'] },
      { agentId: 'frontend-agent', name: '@frontend', type: 'ai-agent', skills: ['frontend', 'react', 'nextjs', 'tailwind', 'ui', 'shadcn', 'typescript'] },
      { agentId: 'reviewer-agent', name: '@code-reviewer', type: 'ai-agent', skills: ['review', 'quality', 'best-practices', 'code-analysis', 'debugging'] },
      { agentId: 'security-agent', name: '@security-auditor', type: 'ai-agent', skills: ['security', 'owasp', 'audit', 'pentest', 'compliance'] },
      { agentId: 'docs-agent', name: '@docs-writer', type: 'ai-agent', skills: ['documentation', 'prd', 'adr', 'writing', 'technical-writing'] },
      { agentId: 'game-agent', name: '@game-dev', type: 'ai-agent', skills: ['game-dev', 'threejs', 'phaser', 'canvas', 'webgl', 'animation'] },
      
      // 🔶 NEW AGENTS
      { agentId: 'intake-agent', name: '@intake', type: 'ai-agent', skills: ['research', 'analysis', 'context-gathering', 'information-retrieval'] },
      { agentId: 'critic-agent', name: '@critic', type: 'ai-agent', skills: ['review', 'critique', 'analysis', 'alternative-approaches'] },
      { agentId: 'handoff-guard', name: '@handoff-guard', type: 'custom', skills: ['monitoring', 'context-tracking', 'session-management'] },
      
      // ⚙️ CODEBUFF SUB-AGENTS
      { agentId: 'code-searcher', name: 'code-searcher', type: 'cli', skills: ['search', 'grep', 'code-search', 'pattern-matching'] },
      { agentId: 'file-picker', name: 'file-picker', type: 'cli', skills: ['file-search', 'fuzzy-find', 'file-discovery'] },
      { agentId: 'researcher-web', name: 'researcher-web', type: 'cli', skills: ['web-research', 'information-retrieval', 'scraping'] },
      { agentId: 'researcher-docs', name: 'researcher-docs', type: 'cli', skills: ['documentation', 'docs-research', 'api-docs'] },
      { agentId: 'thinker-gemini', name: 'thinker-with-files-gemini', type: 'cli', skills: ['deep-thinking', 'analysis', 'complex-problems', 'reasoning'] },
      { agentId: 'basher', name: 'basher', type: 'cli', skills: ['terminal', 'shell', 'commands', 'automation'] },
      { agentId: 'code-reviewer-deepseek', name: 'code-reviewer-deepseek', type: 'cli', skills: ['deep-review', 'code-analysis', 'quality'] },
      
      // 🛠️ SPECIALIZED
      { agentId: 'vault-filler', name: 'vault-filler', type: 'custom', skills: ['obsidian', 'note-filling', 'vault-maintenance', 'research'] },
      { agentId: 'explore', name: 'explore', type: 'opencode', skills: ['search', 'exploration', 'file-discovery', 'codebase-navigation'] },
      { agentId: 'general', name: 'general', type: 'opencode', skills: ['general', 'multi-purpose', 'any-task'] }
    ];
    
    for (const agent of defaultAgents) {
      await Agent.findOneAndUpdate(
        { agentId: agent.agentId },
        { ...agent, status: 'online', lastHeartbeat: new Date() },
        { upsert: true }
      );
    }
    console.log(`✅ ${defaultAgents.length} default agents registered`);
    
    server.listen(PORT, () => {
      console.log(`🏠 Room Server v2 running on http://localhost:${PORT}`);
      console.log(`🔌 WebSocket ready on ws://localhost:${PORT}`);
      console.log(`📊 Stats: http://localhost:${PORT}/api/stats`);
    });
  })
  .catch(err => {
    console.error('❌ MongoDB connection error:', err.message);
    console.log('⚠️ Starting server WITHOUT MongoDB (in-memory mode)');
    server.listen(PORT, () => {
      console.log(`🏠 Room Server v2 running on http://localhost:${PORT} (no DB)`);
    });
  });
