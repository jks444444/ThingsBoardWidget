const admin = require('firebase-admin');
const mqtt = require('mqtt');
const { EventEmitter } = require('events'); // Import EventEmitter

var serviceAccount = require("./DATN/serviceAccountKey.json");
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://appats-b9243-default-rtdb.asia-southeast1.firebasedatabase.app"
});
const mqttServer = 'mqtt://mqtt.thingsboard.cloud';
const GenaccessToken = 'ifbEK6lcTyuy2wAsGFSQ';
const GridaccessToken = 'S7HkkO44ZQSqdVflCBHz';

const genEmitter = new EventEmitter();
const gridEmitter = new EventEmitter();

const genClient = mqtt.connect(mqttServer, {
  username: GenaccessToken
});
const gridClient = mqtt.connect(mqttServer, {
  username: GridaccessToken
});

genClient.setMaxListeners(15); // Increase the limit to 15 listeners
gridClient.setMaxListeners(15); // Increase the limit to 15 listeners

// Variables to store current mode states
let currentMode = { Auto: false, Man: false, Gen: false, Grid: false };
let acceptRemote = false;

// Update currentMode and acceptRemote variables when Firebase changes
const controlRef = admin.database().ref('Control/Mode');
controlRef.on('value', (snapshot) => {
  currentMode = snapshot.val() || { Auto: false, Man: false, Gen: false, Grid: false };
});

const acceptRemoteRef = admin.database().ref('Control/AcceptRemote');
acceptRemoteRef.on('value', (snapshot) => {
  acceptRemote = snapshot.val() || false;
});

// Common function to publish data
function publishData(client, topic, data) {
  if (client.connected) {
    client.publish(topic, JSON.stringify(data), (err) => {
      if (err) {
        console.error('Publish error:', err);
      } else {
        console.log('Data published successfully:', data);
      }
    });
  } else {
    console.error('MQTT client is not connected. Cannot publish data.');
  }
}

// Function to handle mode change and ensure mutual exclusion
function handleModeChange(mode, value) {
  if (!acceptRemote) {
    console.log('Remote control is disabled. Ignoring mode change request.');
    return;
  }

  const update = {};
  if (mode === 'Auto') {
    update.Auto = value;
    if (value) {
      // If Auto is set to true, ensure Man is false
      update.Man = false;
    } else {
      // If Auto is set to false, ensure Man is true
      update.Man = true;
    }
  } else if (mode === 'Man') {
    update.Man = value;
    if (value) {
      // If Man is set to true, ensure Auto is false
      update.Auto = false;
    } else {
      // If Man is set to false, ensure Auto is true
      update.Auto = true;
    }
  } else if (mode === 'Gen') {
    if (currentMode.Man) {
      update['ControlMan/Gen'] = value;
      if (value) {
        // If Gen is set to true, ensure Grid is false
        update['ControlMan/Grid'] = false;
      }
      // No need for else block because Grid can remain false if Gen is false
    } else {
      console.log('Cannot set Gen when Man mode is false.');
      return;
    }
  } else if (mode === 'Grid') {
    if (currentMode.Man) {
      update['ControlMan/Grid'] = value;
      if (value) {
        // If Grid is set to true, ensure Gen is false
        update['ControlMan/Gen'] = false;
      }
      // No need for else block because Gen can remain false if Grid is false
    } else {
      console.log('Cannot set Grid when Man mode is false.');
      return;
    }
  }
  
  controlRef.update(update);
}

// Function to handle time change with additional logging
function handleTimeChange(timeType, value) {
  if (!acceptRemote) {
    console.log('Remote control is disabled. Ignoring time change request.');
    return;
  }

  if (timeType === 'Timegen' || timeType === 'Timegrid') {
    const switchingTimeRef = admin.database().ref('Control/Switchingtime');
    switchingTimeRef.once('value', (snapshot) => {
      const currentSwitchingTime = snapshot.val() || {};
      console.log('Current Switching Time:', currentSwitchingTime);
      currentSwitchingTime[timeType] = value;
      const updateData = {};
      updateData[timeType] = value;
      console.log('Updating Switching Time:', updateData);
      switchingTimeRef.update(updateData, (error) => {
        if (error) {
          console.error(`Time update error for ${timeType}:`, error);
        } else {
          console.log(`Time updated successfully: ${timeType} = ${value}`);
        }
      });
    });
  } else {
    console.error('Invalid time type:', timeType);
  }
}

// Handle RPC requests for Gen client
genClient.on('connect', () => {
  console.log('Connected to MQTT broker for Gen data');

  const database = admin.database();
  const genref = database.ref('Gen');


  genClient.subscribe('v1/devices/me/rpc/request/+');
  genClient.on('message', (topic, message) => {
    const request = JSON.parse(message.toString());
    const method = request.method;
    const params = request.params;

    if (method === 'setAutoMode') {
      handleModeChange('Auto', params);
    } else if (method === 'setManMode') {
      handleModeChange('Man', params);
    } else if (method === 'setGenMode') {
      handleModeChange('Gen', params);
    } else if (method === 'setGridMode') {
      handleModeChange('Grid', params);
    } else if (method === 'setTimeGen') {
      handleTimeChange('Timegen', params);
    } else if (method === 'setTimeGrid') {
      handleTimeChange('Timegrid', params);
    } else if (method === 'setAcceptRemote') {
      acceptRemoteRef.set(params, (error) => {
        if (error) {
          console.error('Error updating AcceptRemote:', error);
        } else {
          console.log('AcceptRemote updated successfully:', params);
        }
      });
    } else if (method === 'getValue') {
      // Send the current mode state without querying Firebase again
      const responseTopic = topic.replace('request', 'response');
      const responseMessage = {
        auto: currentMode.Auto,
        man: currentMode.Man,
        gen: currentMode.Gen,
        grid: currentMode.Grid,
        acceptControl: acceptRemote
      };
      genClient.publish(responseTopic, JSON.stringify(responseMessage), (err) => {
        if (err) {
          console.error('Response publish error:', err);
        } else {
          console.log('RPC response sent:', responseMessage);
        }
      });
    }
  });

  // Update ThingsBoard switches based on Firebase changes
  controlRef.on('value', (snapshot) => {
    const mode = snapshot.val();
    const autoState = mode.Auto || false;
    const manState = mode.Man || false;
    const genState = mode.Gen || false;
    const gridState = mode.Grid || false;

    const autoRpcMessage = { method: "setAutoMode", params: autoState };
    const manRpcMessage = { method: "setManMode", params: manState };
    const genRpcMessage = { method: "setGenMode", params: genState };
    const gridRpcMessage = { method: "setGridMode", params: gridState };

    console.log('Sending RPC notification for control mode (Auto):', autoRpcMessage);
    genClient.publish('v1/devices/me/rpc/request/1', JSON.stringify(autoRpcMessage), (err) => {
      if (err) {
        console.error('Publish error (Auto):', err);
      } else {
        console.log('RPC notification sent (Auto):', autoRpcMessage);
      }
    });

    console.log('Sending RPC notification for control mode (Man):', manRpcMessage);
    genClient.publish('v1/devices/me/rpc/request/2', JSON.stringify(manRpcMessage), (err) => {
      if (err) {
        console.error('Publish error (Man):', err);
      } else {
        console.log('RPC notification sent (Man):', manRpcMessage);
      }
    });

    console.log('Sending RPC notification for control mode (Gen):', genRpcMessage);
    genClient.publish('v1/devices/me/rpc/request/3', JSON.stringify(genRpcMessage), (err) => {
      if (err) {
        console.error('Publish error (Gen):', err);
      } else {
        console.log('RPC notification sent (Gen):', genRpcMessage);
      }
    });

    console.log('Sending RPC notification for control mode (Grid):', gridRpcMessage);
    genClient.publish('v1/devices/me/rpc/request/4', JSON.stringify(gridRpcMessage), (err) => {
      if (err) {
        console.error('Publish error (Grid):', err);
      } else {
        console.log('RPC notification sent (Grid):', gridRpcMessage);
      }
    });
  });

  // Handle time updates
  const switchingTimeRef = database.ref('Control/Switchingtime');
  switchingTimeRef.on('value', (snapshot) => {
    const times = snapshot.val() || {};
    const timegen = times.Timegen || '';
    const timegrid = times.Timegrid || '';

    const timegenRpcMessage = { method: "setTimeGen", params: timegen };
    const timegridRpcMessage = { method: "setTimeGrid", params: timegrid };

    console.log('Sending RPC notification for timegen:', timegenRpcMessage);
    genClient.publish('v1/devices/me/rpc/request/5', JSON.stringify(timegenRpcMessage), (err) => {
      if (err) {
        console.error('Publish error (Timegen):', err);
      } else {
        console.log('RPC notification sent (Timegen):', timegenRpcMessage);
      }
    });

    console.log('Sending RPC notification for timegrid:', timegridRpcMessage);
    genClient.publish('v1/devices/me/rpc/request/6', JSON.stringify(timegridRpcMessage), (err) => {
      if (err) {
        console.error('Publish error (Timegrid):', err);
      } else {
        console.log('RPC notification sent (Timegrid):', timegridRpcMessage);
      }
    });
  });

  genref.on('value', (gensnapshot) => {
    const data = {
      L1_voltage_gen: gensnapshot.child('L1/Voltage').val(),
      L1_current_gen: gensnapshot.child('L1/Current').val(),
      L1_frequency_gen: gensnapshot.child('L1/Frequency').val(),
      L1_energy_gen: gensnapshot.child('L1/Energy').val(),
      L1_power_gen: gensnapshot.child('L1/Power').val(),
      L2_voltage_gen: gensnapshot.child('L2/Voltage').val(),
      L2_current_gen: gensnapshot.child('L2/Current').val(),
      L2_frequency_gen: gensnapshot.child('L2/Frequency').val(),
      L2_energy_gen: gensnapshot.child('L2/Energy').val(),
      L2_power_gen: gensnapshot.child('L2/Power').val(),
      L3_voltage_gen: gensnapshot.child('L3/Voltage').val(),
      L3_current_gen: gensnapshot.child('L3/Current').val(),
      L3_frequency_gen: gensnapshot.child('L3/Frequency').val(),
      L3_energy_gen: gensnapshot.child('L3/Energy').val(),
      L3_power_gen: gensnapshot.child('L3/Power').val(),
      Total_Energy_gen: gensnapshot.child('Total Energy').val(),
      Total_Power_gen: gensnapshot.child('Total Power').val(),
    };

    const topic = 'v1/devices/me/telemetry';
    publishData(genClient, topic, data);
  });
});

// Connect to MQTT broker and read data from Firebase for Grid
gridClient.on('connect', () => {
  console.log('Connected to MQTT broker for Grid data');

  const database = admin.database();
  const mainref = database.ref('Grid');

  mainref.on('value', (mainsnapshot) => {
    const data = {
      L1_voltage: mainsnapshot.child('L1/Voltage').val(),
      L1_current: mainsnapshot.child('L1/Current').val(),
      L1_frequency: mainsnapshot.child('L1/Frequency').val(),
      L1_energy: mainsnapshot.child('L1/Energy').val(),
      L1_power: mainsnapshot.child('L1/Power').val(),
      L2_voltage: mainsnapshot.child('L2/Voltage').val(),
      L2_current: mainsnapshot.child('L2/Current').val(),
      L2_frequency: mainsnapshot.child('L2/Frequency').val(),
      L2_energy: mainsnapshot.child('L2/Energy').val(),
      L2_power: mainsnapshot.child('L2/Power').val(),
      L3_voltage: mainsnapshot.child('L3/Voltage').val(),
      L3_current: mainsnapshot.child('L3/Current').val(),
      L3_frequency: mainsnapshot.child('L3/Frequency').val(),
      L3_energy: mainsnapshot.child('L3/Energy').val(),
      L3_power: mainsnapshot.child('L3/Power').val(),
      Total_Energy: mainsnapshot.child('Total Energy').val(),
      Total_Power: mainsnapshot.child('Total Power').val(),
    };

    const topic = 'v1/devices/me/telemetry';
    publishData(gridClient, topic, data);
  });
});

// Handle errors for Gen client
genClient.on('error', (error) => {
  console.error('Gen MQTT error:', error);
});

genClient.on('offline', () => {
  console.error('Gen MQTT client went offline');
});

genClient.on('reconnect', () => {
  console.log('Gen MQTT client reconnecting');
});

// Handle errors for Grid client
gridClient.on('error', (error) => {
  console.error('Grid MQTT error:', error);
});

gridClient.on('offline', () => {
  console.error('Grid MQTT client went offline');
});

gridClient.on('reconnect', () => {
  console.log('Grid MQTT client reconnecting');
}); 