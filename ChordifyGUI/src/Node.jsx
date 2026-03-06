import React from 'react';
import { Handle, Position } from 'reactflow';

function Node({ data }) {
  // If the node is active, apply these highlighted styles
  const activeStyle = {
    borderColor: '#38bdf8', // Blue primary color
    boxShadow: '0 0 15px rgba(56, 189, 248, 0.6)', // Blue glow
    backgroundColor: '#1e293b', // Slightly lighter background
  };

  // Default styles for unselected nodes
  const defaultStyle = {
    borderColor: '#334155', // Standard border
    boxShadow: 'none',
    backgroundColor: '#0f172a', // Dark background
  };

  return (
    <div 
      // Add the pulse class if the data says it should be pulsing
      className={`node-item ${data.isPulsing ? 'pulse' : ''}`}
      style={{
        width: '70px',
        height: '70px',
        borderRadius: '50%',
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        color: '#f8fafc',
        fontWeight: 'bold',
        fontSize: '1.2em',
        border: '2px solid',
        transition: 'all 0.3s ease', // Smoothly transition the colors when clicked
        cursor: 'pointer', // Show a pointer finger on hover
        ...(data.isActive ? activeStyle : defaultStyle), // Apply the correct styles
      }}
    >
      {/* Invisible Handles so React Flow knows where to attach edges */}
      <Handle type="target" position={Position.Top} style={{ top: '50%', left: '50%', opacity: 0 }} />
      
      <div>{data.id}</div>

      <Handle type="source" position={Position.Bottom} style={{ top: '50%', left: '50%', opacity: 0 }} />
    </div>
  );
}

export default Node;