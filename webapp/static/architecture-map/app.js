import React, { memo, useMemo, useState } from "https://esm.sh/react@18.3.1";
import { createRoot } from "https://esm.sh/react-dom@18.3.1/client";
import ReactFlow, {
  Handle,
  MarkerType,
  Position,
} from "https://esm.sh/reactflow@11.11.4?deps=react@18.3.1,react-dom@18.3.1";

import { ARCHITECTURE_NODES, LAYER_TITLES } from "./config/architecture_nodes.js";
import { ARCHITECTURE_EDGES } from "./config/architecture_edges.js";
import { WORKFLOW_DEFINITIONS } from "./config/workflow_definitions.js";

const STATUS_LABEL = {
  implemented: "implemented",
  partial: "partial",
  planned: "planned",
};

const STATUS_COLORS = {
  implemented: "#1f9d5a",
  partial: "#f39c12",
  planned: "#9aa7bb",
};

const normalize = (value) => String(value || "").trim().toLowerCase();

function statusClass(status) {
  return `status-${status}`;
}

const ModuleNode = memo(({ data }) => {
  return React.createElement(
    "div",
    { className: "module-node" },
    React.createElement(Handle, { type: "target", position: Position.Left, style: { opacity: 0.4 } }),
    React.createElement(
      "div",
      { className: "node-title" },
      data.label,
      " ",
      React.createElement("span", { className: `badge ${statusClass(data.status)}` }, STATUS_LABEL[data.status] || data.status),
    ),
    React.createElement("div", { className: "node-purpose" }, data.purpose || ""),
    React.createElement(Handle, { type: "source", position: Position.Right, style: { opacity: 0.4 } }),
  );
});

const nodeTypes = { module: ModuleNode };

function matchesNode(node, query) {
  const q = normalize(query);
  if (!q) return true;
  const fields = [
    node.label,
    node.purpose,
    node.layer,
    ...(node.inputs || []),
    ...(node.outputs || []),
    ...(node.tables || []),
    ...(node.services || []),
    ...(node.endpoints || []),
    ...(node.caveats || []),
  ]
    .join(" ")
    .toLowerCase();
  return fields.includes(q);
}

function edgeStyleByStatuses(sourceStatus, targetStatus) {
  const hasPlanned = sourceStatus === "planned" || targetStatus === "planned";
  const hasPartial = sourceStatus === "partial" || targetStatus === "partial";
  if (hasPlanned) {
    return { stroke: "#a9b5c7", strokeWidth: 1.3, strokeDasharray: "6 4" };
  }
  if (hasPartial) {
    return { stroke: "#d89a2b", strokeWidth: 1.6, strokeDasharray: "3 3" };
  }
  return { stroke: "#5982da", strokeWidth: 1.8 };
}

function buildArchitectureFlow(nodes, edges) {
  const nodeStatus = Object.fromEntries(nodes.map((n) => [n.id, n.status]));
  const flowNodes = nodes.map((n) => ({
    id: n.id,
    type: "module",
    position: n.position || { x: 0, y: 0 },
    data: {
      label: n.label,
      status: n.status,
      purpose: n.purpose,
    },
  }));

  const nodeIds = new Set(nodes.map((n) => n.id));
  const flowEdges = edges
    .filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target))
    .map((e) => {
      const style = edgeStyleByStatuses(nodeStatus[e.source], nodeStatus[e.target]);
      return {
        ...e,
        animated: false,
        style,
        markerEnd: { type: MarkerType.ArrowClosed, color: style.stroke },
      };
    });

  return { flowNodes, flowEdges };
}

function buildWorkflowFlow(workflow, nodeIndex) {
  const steps = workflow?.steps || [];
  const flowNodes = steps.map((step, idx) => {
    const nodeRef = nodeIndex[step.nodeId] || {};
    return {
      id: step.id,
      type: "module",
      position: { x: 90 + idx * 300, y: 130 },
      data: {
        label: `${idx + 1}. ${step.title}`,
        status: step.status,
        purpose: nodeRef.purpose || step.description || "",
      },
    };
  });

  const flowEdges = steps.slice(1).map((step, idx) => {
    const prev = steps[idx];
    const style = edgeStyleByStatuses(prev.status, step.status);
    return {
      id: `wf-${workflow.id}-${prev.id}-${step.id}`,
      source: prev.id,
      target: step.id,
      style,
      markerEnd: { type: MarkerType.ArrowClosed, color: style.stroke },
    };
  });

  return { flowNodes, flowEdges };
}

function renderList(items, emptyText = "—") {
  if (!items || !items.length) return React.createElement("div", { className: "empty" }, emptyText);
  return React.createElement(
    "ul",
    { className: "list" },
    items.map((x, i) => React.createElement("li", { key: `${x}-${i}` }, x)),
  );
}

function App() {
  const [tab, setTab] = useState("architecture");
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState({
    implemented: true,
    partial: true,
    planned: true,
  });
  const [selectedNodeId, setSelectedNodeId] = useState(ARCHITECTURE_NODES[0]?.id || null);
  const [selectedWorkflowId, setSelectedWorkflowId] = useState(WORKFLOW_DEFINITIONS[0]?.id || null);
  const [selectedWorkflowStepId, setSelectedWorkflowStepId] = useState(null);

  const nodeIndex = useMemo(
    () => Object.fromEntries(ARCHITECTURE_NODES.map((n) => [n.id, n])),
    [],
  );

  const filteredArchitectureNodes = useMemo(
    () =>
      ARCHITECTURE_NODES.filter((n) => statusFilter[n.status]).filter((n) =>
        matchesNode(n, search),
      ),
    [search, statusFilter],
  );

  const { flowNodes, flowEdges } = useMemo(
    () => buildArchitectureFlow(filteredArchitectureNodes, ARCHITECTURE_EDGES),
    [filteredArchitectureNodes],
  );

  const workflowMatches = useMemo(() => {
    const q = normalize(search);
    if (!q) return WORKFLOW_DEFINITIONS;
    return WORKFLOW_DEFINITIONS.filter((wf) => {
      const text = [wf.title, wf.description, ...(wf.steps || []).map((s) => s.title)].join(" ").toLowerCase();
      return text.includes(q);
    });
  }, [search]);

  const selectedNode = nodeIndex[selectedNodeId] || filteredArchitectureNodes[0] || null;
  const selectedWorkflow =
    WORKFLOW_DEFINITIONS.find((w) => w.id === selectedWorkflowId) ||
    WORKFLOW_DEFINITIONS[0] ||
    null;
  const selectedWorkflowStep =
    selectedWorkflow?.steps?.find((s) => s.id === selectedWorkflowStepId) ||
    selectedWorkflow?.steps?.[0] ||
    null;

  const workflowFlow = useMemo(
    () => buildWorkflowFlow(selectedWorkflow, nodeIndex),
    [selectedWorkflow, nodeIndex],
  );

  const statusCounts = useMemo(() => {
    const counts = { implemented: 0, partial: 0, planned: 0 };
    ARCHITECTURE_NODES.forEach((n) => {
      if (counts[n.status] !== undefined) counts[n.status] += 1;
    });
    return counts;
  }, []);

  const sourcesNow = useMemo(
    () =>
      ARCHITECTURE_NODES.filter((n) => n.layer === "data_sources" && n.status === "implemented").map(
        (n) => n.label,
      ),
    [],
  );
  const sourcesPartial = useMemo(
    () =>
      ARCHITECTURE_NODES.filter((n) => n.layer === "data_sources" && n.status === "partial").map(
        (n) => n.label,
      ),
    [],
  );

  const usableNow = useMemo(
    () =>
      ARCHITECTURE_NODES.filter(
        (n) =>
          n.status === "implemented" &&
          ["storage", "decision", "creative_production"].includes(n.layer),
      ).map((n) => n.label),
    [],
  );
  const missingForFullLoop = useMemo(
    () =>
      ARCHITECTURE_NODES.filter((n) => n.status === "planned" && n.layer === "future").map(
        (n) => n.label,
      ),
    [],
  );

  const handleStatusToggle = (key) => {
    setStatusFilter((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  return React.createElement(
    "div",
    { className: "map-shell" },
    React.createElement(
      "aside",
      { className: "left-panel panel-pad" },
      React.createElement(
        "a",
        { href: "/admin", className: "top-link" },
        React.createElement("i", { className: "ti ti-arrow-left" }),
        "Вернуться в админку",
      ),
      React.createElement("h1", { className: "title" }, "Architecture + Workflow Map"),
      React.createElement(
        "p",
        { className: "subtitle" },
        "Реальная карта текущей системы: что внедрено, что частично, что запланировано.",
      ),
      React.createElement("div", { className: "field-label" }, "Поиск по модулю / сущности / workflow"),
      React.createElement("input", {
        className: "input",
        value: search,
        placeholder: "Например: prediction, VK, launch queue",
        onChange: (e) => setSearch(e.target.value),
      }),
      React.createElement(
        "div",
        { className: "check-grid" },
        ["implemented", "partial", "planned"].map((key) =>
          React.createElement(
            "label",
            { key, className: "check-row" },
            React.createElement("input", {
              type: "checkbox",
              checked: !!statusFilter[key],
              onChange: () => handleStatusToggle(key),
            }),
            React.createElement("span", { className: `badge ${statusClass(key)}` }, STATUS_LABEL[key]),
          ),
        ),
      ),
      React.createElement(
        "div",
        { className: "stats-grid" },
        React.createElement(
          "div",
          { className: "stat-card" },
          React.createElement("div", { className: "field-label" }, "Implemented"),
          React.createElement("b", null, statusCounts.implemented),
        ),
        React.createElement(
          "div",
          { className: "stat-card" },
          React.createElement("div", { className: "field-label" }, "Partial"),
          React.createElement("b", null, statusCounts.partial),
        ),
        React.createElement(
          "div",
          { className: "stat-card" },
          React.createElement("div", { className: "field-label" }, "Planned"),
          React.createElement("b", null, statusCounts.planned),
        ),
      ),
      tab === "workflows" &&
        React.createElement(
          "div",
          { className: "legend" },
          React.createElement("div", { className: "field-label" }, "Workflow list"),
          React.createElement(
            "div",
            { className: "workflow-list" },
            workflowMatches.map((wf) =>
              React.createElement(
                "button",
                {
                  key: wf.id,
                  className: `wf-item${selectedWorkflowId === wf.id ? " active" : ""}`,
                  onClick: () => {
                    setSelectedWorkflowId(wf.id);
                    setSelectedWorkflowStepId(wf.steps?.[0]?.id || null);
                  },
                },
                React.createElement("div", { style: { fontWeight: 700, marginBottom: "4px" } }, wf.title),
                React.createElement("div", { style: { color: "#607088" } }, wf.description),
              ),
            ),
          ),
        ),
      React.createElement(
        "div",
        { className: "legend" },
        React.createElement("div", { className: "field-label" }, "Reasonable sufficiency"),
        React.createElement(
          "ul",
          { className: "list" },
          React.createElement("li", null, "No blind autonomous launch queue in production loop."),
          React.createElement("li", null, "No fake multi-source “AI brain” abstraction."),
          React.createElement("li", null, "No duplicate subsystems for the same function."),
          React.createElement("li", null, "Only modules with clear operational value are implemented."),
        ),
      ),
    ),
    React.createElement(
      "main",
      { className: "main-panel" },
      React.createElement(
        "div",
        { className: "top-controls" },
        React.createElement(
          "button",
          { className: `tab-btn${tab === "architecture" ? " active" : ""}`, onClick: () => setTab("architecture") },
          "Architecture",
        ),
        React.createElement(
          "button",
          { className: `tab-btn${tab === "workflows" ? " active" : ""}`, onClick: () => setTab("workflows") },
          "Workflows",
        ),
        React.createElement(
          "button",
          { className: `tab-btn${tab === "status" ? " active" : ""}`, onClick: () => setTab("status") },
          "Current Status",
        ),
      ),
      tab === "architecture" &&
        React.createElement(
          "div",
          { className: "canvas-wrap" },
          React.createElement(ReactFlow, {
            nodes: flowNodes,
            edges: flowEdges,
            nodeTypes,
            fitView: true,
            minZoom: 0.35,
            maxZoom: 2,
            onNodeClick: (_e, node) => setSelectedNodeId(node.id),
          }),
        ),
      tab === "workflows" &&
        React.createElement(
          "div",
          { className: "canvas-wrap" },
          React.createElement(ReactFlow, {
            nodes: workflowFlow.flowNodes,
            edges: workflowFlow.flowEdges,
            nodeTypes,
            fitView: true,
            minZoom: 0.4,
            maxZoom: 2,
            onNodeClick: (_e, node) => setSelectedWorkflowStepId(node.id),
          }),
        ),
      tab === "status" &&
        React.createElement(
          "div",
          { className: "panel-pad", style: { overflow: "auto" } },
          React.createElement("h2", { className: "detail-title" }, "Operational summary: что можно использовать сейчас"),
          React.createElement(
            "div",
            { className: "detail-block" },
            React.createElement("div", { className: "field-label" }, "Connected sources now"),
            renderList(sourcesNow, "Нет подключённых источников"),
            React.createElement("div", { className: "field-label", style: { marginTop: 10 } }, "Partially connected sources"),
            renderList(sourcesPartial, "Нет partial источников"),
          ),
          React.createElement(
            "div",
            { className: "detail-block" },
            React.createElement("div", { className: "field-label" }, "Production-usable modules"),
            renderList(usableNow),
          ),
          React.createElement(
            "div",
            { className: "detail-block" },
            React.createElement("div", { className: "field-label" }, "Missing for practical end-to-end loop"),
            renderList(missingForFullLoop),
          ),
          React.createElement(
            "div",
            { className: "detail-block" },
            React.createElement("div", { className: "field-label" }, "Intentionally minimal (anti-overengineering)"),
            React.createElement(
              "ul",
              { className: "list" },
              React.createElement("li", null, "No launch queue automation yet."),
              React.createElement("li", null, "No platform export automation yet."),
              React.createElement("li", null, "No auto-pause/auto-scale logic yet."),
              React.createElement("li", null, "No autonomous creative rewriting loops yet."),
            ),
          ),
        ),
    ),
    React.createElement(
      "aside",
      { className: "right-panel panel-pad" },
      tab === "architecture" &&
        (selectedNode
          ? React.createElement(
              React.Fragment,
              null,
              React.createElement("h3", { className: "detail-title" }, selectedNode.label),
              React.createElement(
                "div",
                { className: "detail-block" },
                React.createElement("div", { className: `badge ${statusClass(selectedNode.status)}` }, STATUS_LABEL[selectedNode.status]),
                React.createElement("p", { className: "kv" }, selectedNode.purpose),
                React.createElement("div", { className: "kv" }, `Layer: ${LAYER_TITLES[selectedNode.layer] || selectedNode.layer}`),
              ),
              React.createElement(
                "div",
                { className: "detail-block" },
                React.createElement("div", { className: "field-label" }, "Why module exists"),
                React.createElement("div", { className: "kv" }, selectedNode.purpose || "—"),
                React.createElement("div", { className: "field-label", style: { marginTop: 8 } }, "Inputs"),
                renderList(selectedNode.inputs),
                React.createElement("div", { className: "field-label", style: { marginTop: 8 } }, "Outputs"),
                renderList(selectedNode.outputs),
              ),
              React.createElement(
                "div",
                { className: "detail-block" },
                React.createElement("div", { className: "field-label" }, "Related tables/entities"),
                renderList(selectedNode.tables),
                React.createElement("div", { className: "field-label", style: { marginTop: 8 } }, "Related services/modules"),
                renderList(selectedNode.services),
                React.createElement("div", { className: "field-label", style: { marginTop: 8 } }, "Related API endpoints"),
                renderList(selectedNode.endpoints),
              ),
              React.createElement(
                "div",
                { className: "detail-block" },
                React.createElement("div", { className: "field-label" }, "Status notes / caveats"),
                renderList(selectedNode.caveats),
                React.createElement("div", { className: "field-label", style: { marginTop: 8 } }, "Depends on"),
                renderList(selectedNode.depends_on),
                React.createElement("div", { className: "field-label", style: { marginTop: 8 } }, "Feeds into"),
                renderList(selectedNode.feeds_into),
              ),
            )
          : React.createElement("div", { className: "empty" }, "Выберите узел на карте.")),
      tab === "workflows" &&
        (selectedWorkflow
          ? React.createElement(
              React.Fragment,
              null,
              React.createElement("h3", { className: "detail-title" }, selectedWorkflow.title),
              React.createElement("p", { className: "kv" }, selectedWorkflow.description),
              React.createElement(
                "div",
                { className: "detail-block" },
                React.createElement("div", { className: "field-label" }, "Workflow steps"),
                selectedWorkflow.steps.map((step, idx) =>
                  React.createElement(
                    "div",
                    {
                      key: step.id,
                      className: "step-row",
                      onClick: () => setSelectedWorkflowStepId(step.id),
                      style: { cursor: "pointer", borderColor: selectedWorkflowStep?.id === step.id ? "#b9caf2" : undefined },
                    },
                    React.createElement(
                      "div",
                      { style: { display: "flex", justifyContent: "space-between", gap: "8px", alignItems: "center" } },
                      React.createElement("b", null, `${idx + 1}. ${step.title}`),
                      React.createElement("span", { className: `badge ${statusClass(step.status)}` }, STATUS_LABEL[step.status]),
                    ),
                    React.createElement("div", { className: "kv" }, `Module: ${nodeIndex[step.nodeId]?.label || step.nodeId}`),
                    step.caveat && React.createElement("div", { className: "kv" }, `Caveat: ${step.caveat}`),
                  ),
                ),
              ),
              selectedWorkflowStep &&
                React.createElement(
                  "div",
                  { className: "detail-block" },
                  React.createElement("div", { className: "field-label" }, "Selected step detail"),
                  React.createElement("div", { className: "kv" }, selectedWorkflowStep.title),
                  React.createElement("div", { className: "kv" }, `Status: ${STATUS_LABEL[selectedWorkflowStep.status]}`),
                  selectedWorkflowStep.caveat &&
                    React.createElement("div", { className: "kv" }, `Caveat: ${selectedWorkflowStep.caveat}`),
                ),
            )
          : React.createElement("div", { className: "empty" }, "Workflow не найден.")),
      tab === "status" &&
        React.createElement(
          React.Fragment,
          null,
          React.createElement("h3", { className: "detail-title" }, "Live-readiness snapshot"),
          React.createElement(
            "div",
            { className: "status-summary-list" },
            React.createElement(
              "div",
              { className: "detail-block" },
              React.createElement("div", { className: "field-label" }, "Implemented now"),
              renderList(ARCHITECTURE_NODES.filter((n) => n.status === "implemented").map((n) => n.label)),
            ),
            React.createElement(
              "div",
              { className: "detail-block" },
              React.createElement("div", { className: "field-label" }, "Partial"),
              renderList(ARCHITECTURE_NODES.filter((n) => n.status === "partial").map((n) => n.label)),
            ),
            React.createElement(
              "div",
              { className: "detail-block" },
              React.createElement("div", { className: "field-label" }, "Planned"),
              renderList(ARCHITECTURE_NODES.filter((n) => n.status === "planned").map((n) => n.label)),
            ),
          ),
        ),
    ),
  );
}

const root = createRoot(document.getElementById("architecture-map-root"));
root.render(React.createElement(App));
