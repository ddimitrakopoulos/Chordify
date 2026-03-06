import { useState, useEffect } from 'react';
import axios from 'axios';
import sha1 from 'js-sha1';
import './App.css';
import NetworkGraph from './NetworkGraph';

function App() {
  // Bootstrap
  const bootstrapNode = {
    addr: '127.0.0.1:8000',
    api: 'http://127.0.0.1:18000'
  };
  const [connectedToBootstrap, setConnectedToBootstrap] = useState(false);

  // Function to check connectivity to the bootstrap node and get the network topology
  // We sent an OVERLAY request to the bootstrap and if it responds we consider it connected, otherwise it's offline.
  const updateConnectionStatusAndOverlay = async () => {
    try {
      const result = await axios({
        method: 'get',
        url: `${bootstrapNode.api}/overlay`,
        timeout: 100,
      }); 
      setConnectedToBootstrap(true);

      // Sort the nodes by ID before setting the topology state to ensure consistent ordering in the UI
      const sorted_topology = result.data.sort((a, b) => a[0] - b[0]);
      setTopology(sorted_topology);
      console.log(sorted_topology);
    } 
    catch (err) {
      setConnectedToBootstrap(false);
      console.warn("Bootstrap node is offline or unreachable");

      // Clear topology if bootstrap is unreachable
      setTopology([]); 
    }
  };

  // Node
  // This state holds the currently active node that the user is controlling.
  // By default, it starts with the bootstrap node, but the user can click on any node to switch control to it.
  // All API requests (insert, query, delete, depart) will be sent to the active node's API endpoint.
  // The overlay and ping requests will always target the bootstrap node to ensure we can fetch the network
  // topology and check connectivity regardless of which node is active.
  const [activeNode, setActiveNode] = useState({ 
    addr: '127.0.0.1:8000', 
    api: 'http://127.0.0.1:18000' 
  });
  const nodeRequest = (method, url, data = null) => {
    if (url !== '/overlay') {
      return axios({
        method,
        url: `${activeNode.api}${url}`,
        data,
        timeout: 2000,
      })
    }
    else {
      // For overlay and ping, always target the bootstrap node
      return axios({
        method,
        url: `${bootstrapNode.api}${url}`,
        data,
        timeout: 2000,
      });
    }
  };

  // Function to switch nodes
  const connectToNode = (nodeAddr) => {
    const ip = nodeAddr.split(':')[0];
    const port = parseInt(nodeAddr.split(':')[1]);
    const apiPort = port + 10000;
    
    setActiveNode({
      addr: nodeAddr,
      api: `http://${ip}:${apiPort}`
    });
    
    updateStatus('info', `Connected to ${nodeAddr}`);
    // Refresh data for the new node
    setResults([]); 
  };

  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [deleteKey, setDeleteKey] = useState('');
  const [queryKey, setQueryKey] = useState('*');
  const [results, setResults] = useState([]);
  const [topology, setTopology] = useState([]);
  const [status, setStatus] = useState({ type: 'info', msg: 'System Ready' });

  // --- API Actions ---

  const handleInsert = async (e) => {
    e.preventDefault();

    if (activeNode.addr === bootstrapNode.addr) {
      updateStatus('error', 'Cannot perform insert on bootstrap node. Please select a different node.');
      return;
    }

    try {
      await nodeRequest('post', '/insert', { key, value });
      updateStatus('success', `Successfully inserted "${key}"`);
      setKey(''); setValue('');
    } catch (err) {
      updateStatus('error', 'Insert failed: Connection refused');
    }
  };

  const handleQuery = async () => {
    if (activeNode.addr === bootstrapNode.addr) {
      updateStatus('error', 'Cannot perform query on bootstrap node. Please select a different node.');
      return;
    }

    try {
      const res = await nodeRequest('get', `/query/${encodeURIComponent(queryKey)}`);
      setResults(res.data);
      updateStatus('info', `Query returned ${res.data.length} hash slots`);
    } catch (err) {
      updateStatus('error', 'Query failed');
    }
  };

  const handleDelete = async (targetKey) => {
    if (activeNode.addr === bootstrapNode.addr) {
      updateStatus('error', 'Cannot perform delete on bootstrap node. Please select a different node.');
      return;
    }

    try {
      await nodeRequest('delete', `/delete/${encodeURIComponent(targetKey)}`);
      updateStatus('success', `Deleted ${targetKey}`);
      handleQuery();
    } catch (err) {
      updateStatus('error', 'Delete failed');
    }
  };

  const handleDepart = async () => {
    if (activeNode.addr === bootstrapNode.addr) {
      updateStatus('error', 'Cannot perform depart on bootstrap node. Please select a different node.');
      return;
    }

    if (!window.confirm("Are you sure you want this node to leave the ring?")) return;

    try {
      updateStatus('info', 'Initiating departure...');
      
      // Send the depart request to the Axum backend
      await nodeRequest('post', '/depart');
      
      updateStatus('success', 'Node departed successfully. This interface is now disconnected.');
      
      // Clear state since the node is no longer part of the network
      setTopology([]);
      setResults([]);
    } catch (err) {
      updateStatus('error', 'Departure failed: ' + (err.response?.data || err.message));
    }
  };

  // HELPERS
  // Hashing 
 /*
  * Synchronously hashes a string and returns a BigInt mapped to an N-bit space.
  * @param {string} data - The string to hash
  * @returns {bigint}
  */
  const hashValue = (data) => {
    // Get the hash result as an array of bytes
    const hashBytes = sha1.array(data);

    // Convert the first 8 bytes to a u64 BigInt (Big-Endian)
    let hashValueInt = 0n;
    for (let i = 0; i < 8; i++) {
      // Shift left by 8 bits and bitwise OR the next byte
      hashValueInt = (hashValueInt << 8n) | BigInt(hashBytes[i]);
    }

    // Calculate modulo (1 << 10)
    const nBigInt = BigInt(10);
    const moduloSpace = 1n << nBigInt; 

    return hashValueInt % moduloSpace;
  }

  // This state is used to force re-render of the status message for animation purposes
  const [actionID, setActionID] = useState(Date.now()); 

  const updateStatus = (type, msg) => {
    setStatus({ type, msg });
    setActionID(Date.now()); // Update actionID to trigger re-render of status message
  }

  // This effect runs only once on mount
  useEffect(() => {
    updateConnectionStatusAndOverlay();
    const interval1 = setInterval(updateConnectionStatusAndOverlay, 200);

    // fetchOverlay();
    // const interval2 = setInterval(fetchOverlay, 1000);
    return () => {
      clearInterval(interval1);
      // clearInterval(interval2);
    }
  }, []);


  return (
    <div className="app-container">

      <header className="app-header">
        <h1>Chordify<span>Graphical User Interface</span></h1>

        <div className="status-bar">
          <div key={actionID} className={`status ${status.type}`}>{status.msg}</div>

          <div key={connectedToBootstrap} className={`status ${connectedToBootstrap ? 'success' : 'error'}`}>
            {connectedToBootstrap ? 'Connected to Bootstrap' : 'Disconnected from Bootstrap'}
            <div className={`dot ${connectedToBootstrap ? 'online' : 'offline'}`}></div>
          </div>
          
        </div>

      </header>

      <main className="dashboard-grid">
        {/* INSERT Section */}
        <section className="panel">
          <div className="panel-title">Insert Data</div>
          <div className="panel-body">
            <form className="entry-form" onSubmit={handleInsert}>
              <div className="input-group">
                <label style={{paddingRight: '13px'}}>Key</label>
                <input value={key} onChange={e => setKey(e.target.value)} placeholder="e.g. Blood upon the snow" required />
              </div>
              <div className="input-group">
                <label>Value</label>
                <input value={value} onChange={e => setValue(e.target.value)} placeholder="e.g. 42" required />
              </div>
              <button type="submit" className="btn-primary">Execute Insert</button>
            </form>
          </div>
        </section>

        {/* DELETE Section */}
        <section className="panel">
          <div className="panel-title">Delete Data</div>
          <div className="panel-body">
            <div className="input-group">
              <label>Key</label>
              <input value={deleteKey} onChange={e => setDeleteKey(e.target.value)} placeholder="e.g. Diary of Jane" required />
            </div>
            <button type="button" className="btn-danger" onClick={() => handleDelete(deleteKey)}>Execute Delete</button>
          </div>
        </section>

        {/* DEPART Section */}
        <section className="panel">
          <div className="panel-title">Depart</div>
          <div className="panel-body">
            <button className="btn-danger" onClick={handleDepart}>
              Execute Depart on Selected Node
            </button>
          </div>
        </section>

        {/* QUERY Section */}
        <section className="panel query-panel">
          <div className="panel-header">
            <div className="panel-title">DHT</div>
            <div className="search-box">
              <input value={queryKey} onChange={e => setQueryKey(e.target.value)} />
              <button onClick={handleQuery}>Query</button>
            </div>
          </div>

          <div className="panel-body">
            <div className="table-wrapper">
              <table>
                <thead>
                  <tr>
                    <th>Node ID</th>
                    <th>Stored Values</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map(([hash, vals]) => (
                    <tr key={hash}>
                      <td className="mono">{hash}</td>
                      <td>{vals.length > 0 ? vals.map((val, _) => {
                        // add the hash of the value next to it
                        const key = val.split(':')[0];
                        const value = val.split(':')[1];
                        
                        const keyHash = hashValue(key, 10); 

                        return key + ':' + value + ` (hash: ${keyHash})`;

                      }).join(', ') : <span className="empty">empty</span>}</td>
                      <td>
                        <button className="btn-danger" onClick={() => handleDelete(queryKey)}>Delete</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>


        {/* OVERLAY Section */}
        <section className="panel ring-panel">
          <div className="panel-body">
            <NetworkGraph topology={topology} activeNode={activeNode} connectToNode={connectToNode} />
          </div>
        </section>

        <section className="panel ring-panel">
          <div className="panel-header">
            <div className="panel-title">Network Topology</div>
          </div>

          <div className="panel-body">
            <div className="node-ring">
              {topology.map(([id, addr]) => (
                <div 
                  key={id} 
                  className={`node-item clickable ${addr === activeNode.addr ? 'active' : ''}`}
                  onClick={() => connectToNode(addr)}
                >
                  <div className="node-info">
                    <span className="node-id">{id != 0 ? ('ID ' + id) : 'BOOTSTRAP'}</span>
                    <span className="node-addr">{addr}</span>
                  </div>
                  {addr === activeNode.addr && <span className="control-indicator">Controlling</span>}
                </div>
              ))}
            </div>
          </div>
        </section>

      </main>
    </div>
  );
}

export default App;