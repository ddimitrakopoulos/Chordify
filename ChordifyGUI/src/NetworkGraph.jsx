import React, { useState, useMemo, useEffect, useCallback } from 'react';
import ReactFlow, { Background, Controls, MarkerType, BaseEdge } from 'reactflow';
import 'reactflow/dist/style.css';
import Node from './Node.jsx';

// CUSTOM EDGE: Pure Math Circular Arc
function ArcEdge({ data, style, markerEnd }) {
  const { angle1, angle2 } = data;
  const R = 200; // MUST match the layout radius exactly
  const centerX = 300; 
  const centerY = 300;
  const nodeRadius = 35; // Half of the 70px node width
  
  // Calculate angle offset so the line stops exactly at the node's edge + 8px for the arrow
  const delta = (nodeRadius + 8) / R; 

  let startAngle = angle1 + delta;
  let a2 = angle2;

  // If the edge crosses the 12 o'clock mark, or if it's a 1-node self-loop, wrap the angle
  if (a2 <= angle1 + 0.001) {
    a2 += 2 * Math.PI;
  }
  
  let endAngle = a2 - delta;

  // Calculate perfect exact coordinates on the circle
  const startX = centerX + R * Math.cos(startAngle);
  const startY = centerY + R * Math.sin(startAngle);
  
  const endX = centerX + R * Math.cos(endAngle);
  const endY = centerY + R * Math.sin(endAngle);

  // Large arc flag: '1' if the angle spans more than 180 degrees (e.g., self-loops)
  const diff = endAngle - startAngle;
  const largeArcFlag = diff > Math.PI ? 1 : 0;

  // SVG Arc: A radiusX radiusY xAxisRotation largeArcFlag sweepFlag endX endY
  const path = `M ${startX} ${startY} A ${R} ${R} 0 ${largeArcFlag} 1 ${endX} ${endY}`;

  return <BaseEdge path={path} markerEnd={markerEnd} style={style} />;
}


// --- FIXED RING GENERATOR ---
function generateChordRing(topology, activeNode) {
  const nodes = [];
  const edges = [];
  
  const radius = 200; 
  const centerX = 300; 
  const centerY = 300;

  // Remove the bootstrap node (ID 0)
  const filteredTopology = topology.filter(peer => peer[0] !== 0);

  // Strictly sort by ID for a proper Chord ring
  const sortedTopology = [...filteredTopology].sort((a, b) => a[0] - b[0]);

  sortedTopology.forEach((peer, index) => {
    const [id, address] = peer; 
    
    // Check if THIS node's address matches the activeNode's address
    const isActiveNode = address === activeNode?.addr;
    
    // Calculate the start angle
    const angle1 = (index / sortedTopology.length) * 2 * Math.PI - (Math.PI / 2);
    
    nodes.push({
      id: `node-${id}`,
      type: 'Node',
      selected: isActiveNode, // Uses the fixed boolean
      position: {
        x: centerX + radius * Math.cos(angle1) - 35, 
        y: centerY + radius * Math.sin(angle1) - 35,
      },
      data: { 
        id: id, 
        address: address, 
        isActive: isActiveNode,  // Uses the fixed boolean
        isPulsing: isActiveNode  // Uses the fixed boolean
      },
    });

    // Calculate the target angle for the successor
    const nextIndex = (index + 1) % sortedTopology.length;
    const [successorId] = sortedTopology[nextIndex];
    const angle2 = (nextIndex / sortedTopology.length) * 2 * Math.PI - (Math.PI / 2);

    edges.push({
      id: `edge-${id}-${successorId}`,
      source: `node-${id}`,
      target: `node-${successorId}`,
      type: 'arc', 
      data: { angle1, angle2 }, 
      animated: false,
      selected: isActiveNode,
      style: { 
        stroke: '#38bdf8', 
        strokeWidth: 2,
        strokeDasharray: '6,6' 
      },
    });
  });

  // Add the bootstrap node in the center
  const bootstrapNode = topology.find(peer => peer[0] === 0);
  if (bootstrapNode) {
    const [id, address] = bootstrapNode;
    const isActiveNode = address === activeNode?.addr;
    
    nodes.push({
      id: `node-${id}`,
      type: 'Node',
      selected: isActiveNode, // Uses the fixed boolean
      position: { x: centerX - 35, y: centerY - 35 }, 
      data: { 
        id: id,
        address: address,
        isActive: isActiveNode, // Uses the fixed boolean
        isPulsing: isActiveNode // Uses the fixed boolean
      },
    });
  }

  return { nodes, edges };
}


function NetworkGraph({ topology = [], activeNode, connectToNode }) {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  
  const nodeTypes = useMemo(() => ({ Node: Node }), []);
  const edgeTypes = useMemo(() => ({ arc: ArcEdge }), []); 

  useEffect(() => {
    if (!topology || topology.length === 0) return; 

    // We pass the whole activeNode object now
    const { nodes: newNodes, edges: newEdges } = generateChordRing(topology, activeNode);
    setNodes(newNodes);
    setEdges(newEdges);
  }, [topology, activeNode]); // This will re-trigger whenever activeNode changes

  const onNodeClick = useCallback((event, node) => {
    // You are correctly passing the address to App.jsx here!
    connectToNode(node.data.address); 
  }, [connectToNode]);

  return (
    <div style={{ width: '100%', height: '600px' }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes} 
        onNodeClick={onNodeClick} 
        fitView
      >
        <Background color="#334155" gap={16} />
        <Controls />
      </ReactFlow>
    </div>
  );
}

export default NetworkGraph;