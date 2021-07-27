# Drone handler:
package com.odafa.dronecloudapp.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.odafa.dronecloudapp.dto.DataPoint;
import com.odafa.dronecloudapp.dto.DroneInfo;
import com.odafa.dronecloudapp.utils.DataMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DroneHandler {
	private static final long MAX_WAIT_TIME = 10_000L;
	private final String droneId;

	private volatile long lastUpdateTime;
	private DroneInfo lastStatus;
	private  String latestDroneConsoleLogMessage;

	private final Socket droneSocket;
	private final InputStream streamIn;
	private final OutputStream streamOut;
	private final InputStream clientLogStreamIn;
	private final Socket clientLogStreamerSocket;
	
	private final BlockingQueue<byte[]> indoxMessageBuffer;
	private final BlockingQueue<String> clientConsoleLogMessageBuffer;
	private final ExecutorService handlerExecutor;
	private final ControlManager manager;

    public DroneHandler(ControlManager controlManager, Socket clientSocket, Socket clientLogStreamer) {
		this.manager = controlManager;
		this.droneSocket = clientSocket;
		this.clientLogStreamerSocket = clientLogStreamer;
		this.indoxMessageBuffer = new ArrayBlockingQueue<>(1024);
		this.clientConsoleLogMessageBuffer = new ArrayBlockingQueue<>(1024);
		this.handlerExecutor = Executors.newFixedThreadPool(3);

		try {
			this.streamIn  = droneSocket.getInputStream();
			this.streamOut = droneSocket.getOutputStream();
			this.clientLogStreamIn = clientLogStreamerSocket.getInputStream();
			droneId = DataMapper.extractDroneIdFromNetwork(droneSocket);
		} catch (Exception e) {
			close();
			throw new RuntimeException(e);
		}
		manager.setControlHadlerForDroneId(droneId, this);
		log.info("Control Connection Established ID {}, IP {} ", droneId, droneSocket.getInetAddress().toString());
    }

    public void activate() {
		
		handlerExecutor.execute( () -> {
			while (!droneSocket.isClosed()) {
				try {
					this.lastStatus = DataMapper.fromNetworkToDroneInfo(streamIn);
					this.lastUpdateTime = System.currentTimeMillis();
				} catch (Exception e) {
					log.info("Control Connection with {} Closed, reason: {}", droneSocket.getInetAddress().toString(), e.getMessage());
					close();
				}
			}
			close();
		});

		handlerExecutor.execute( () -> {
			while (!droneSocket.isClosed()) {
				try {
					streamOut.write( indoxMessageBuffer.take());
					streamOut.flush();
				} catch (SocketException se) {
					log.info("Socket has been closed: {}", se.getMessage());
					close();
				} catch (Exception e) {
					log.error(e.getMessage());
				}
			}
		});
    }

	public void startLogStreaming()
	{

		handlerExecutor.execute(()->{
			while (!clientLogStreamerSocket.isClosed()) {
				try {
					 this.latestDroneConsoleLogMessage = DataMapper.fromNetworkMessageToDroneConsoleLog(clientLogStreamIn);
					 this.clientConsoleLogMessageBuffer.put(this.latestDroneConsoleLogMessage);
					//this.lastUpdateTime = System.currentTimeMillis();
				} catch (Exception e) {
					log.info("Log streaming socket connection with {} closed, reason: {}", clientLogStreamerSocket.getInetAddress().toString(), e.getMessage());
					close();
				}
			}
			close();
		});

	}

    public void sendMissionData(List<DataPoint> dataPoints) {
		final byte[] message = DataMapper.toNetworkMessage(dataPoints);
		this.indoxMessageBuffer.add(message);

		log.debug("Sending Mission Data: {}", dataPoints);
    }

    public void sendCommand(int commandCode) {
		final byte[] message = DataMapper.toNetworkMessage(commandCode);
		this.indoxMessageBuffer.add(message);
		
		log.debug("Sending Command Code: {} For Drone ID {}", commandCode, droneId);
    }

    public DroneInfo getDroneLastStatus() {
		if (isMaxWaitTimeExceeded()) {
			log.warn("Maximum Wait Time for Drone ID {} exceeded. Control socket closed", droneId);
			close();
		}
		return this.lastStatus;
    }
   
	public String getLatestClientConsoleLogMessage()
   {
     return this.latestDroneConsoleLogMessage;
   }

   public BlockingQueue<String> getClientConsoleLogMessageQueue()
   {
     return this.clientConsoleLogMessageBuffer;
   }

	private boolean isMaxWaitTimeExceeded() {
		return System.currentTimeMillis() - lastUpdateTime > MAX_WAIT_TIME;
	}

	private void close() {
		try {
			droneSocket.close();
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		try {
			streamIn.close();
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		try {
			streamOut.close();
		} catch (Exception e) {
			log.error(e.getMessage());
		} 
		try {
			clientLogStreamerSocket.close();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		try {
			clientLogStreamIn.close();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		manager.removeControlHadlerForDroneId(droneId);
		handlerExecutor.shutdownNow(); 
	}
}

  
  control man:
  package com.odafa.dronecloudapp.service;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.odafa.dronecloudapp.configuration.ConfigReader;
import com.odafa.dronecloudapp.dto.DataPoint;
import com.odafa.dronecloudapp.dto.DroneInfo;
import com.odafa.dronecloudapp.utils.DataMapper;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ControlManager implements Runnable {
	private final ServerSocket serverSocket;
	private final ServerSocket logStreamerSocket;
	private final ExecutorService serverRunner;
	
	private final Map<String, DroneHandler> droneIdToHandler;
	
	public ControlManager(ConfigReader configurations) {
		try {
			serverSocket = new ServerSocket( configurations.getControlServerPort());
			logStreamerSocket = new ServerSocket(configurations.getLogStreamerPort());
		} catch (IOException e) {
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}
		serverRunner = Executors.newSingleThreadExecutor();
		droneIdToHandler = new ConcurrentHashMap<>();

		serverRunner.execute(this);
	} 
	
	public void run() {
		while (!serverSocket.isClosed() && !logStreamerSocket.isClosed()) {
			try 
			{
				Socket clientSocket = serverSocket.accept();
				Socket clientLogStreamerSocket = logStreamerSocket.accept();//Listens for a connection to be made to this socket and accepts it. The method blocks until a connection is made. 

				final DroneHandler handler = new DroneHandler(this, clientSocket, clientLogStreamerSocket);
				handler.activate();
				String droneIdFromClientLogStreamer = DataMapper.extractDroneIdFromNetwork(clientLogStreamerSocket);
				while(true)
				{
					if(droneIdToHandler.containsKey(droneIdFromClientLogStreamer))
					{
						droneIdToHandler.get(droneIdFromClientLogStreamer).startLogStreaming();
						break;
					}
				}

			}
			catch (Exception e) 
			{
				log.error(e.getMessage());
			}
		}
	}

    public void sendMissionDataToDrone(String droneId, List<DataPoint> dataPoints) {
		final DroneHandler handler = droneIdToHandler.get(droneId);
		if(handler != null) {
			handler.sendMissionData(dataPoints);
		}
    }

    public void sendMessageFromUserIdToDrone(String droneId, int commandCode) {
		final DroneHandler handler = droneIdToHandler.get(droneId);
		if(handler != null) {
			handler.sendCommand(commandCode);
		}
    }

    public List<DroneInfo> getDroneStatusAll() {
		List<DroneInfo> drones = new ArrayList<>();

		droneIdToHandler.values().forEach( handler -> {
			drones.add(handler.getDroneLastStatus());
		});
		return drones;
    }
	
	public void setControlHadlerForDroneId(String droneId, DroneHandler handler) {
		droneIdToHandler.put(droneId, handler);
	}
	
	public void removeControlHadlerForDroneId(String droneId) {
		droneIdToHandler.remove(droneId);
	}
    
}

  data mapper:
  package com.odafa.dronecloudapp.utils;

import java.io.InputStream;
import java.net.Socket;
import java.util.List;

import com.odafa.dronecloudapp.dto.DataPoint;
import com.odafa.dronecloudapp.dto.DroneInfo;

public class DataMapper {

    private static final int START_MISSION_CODE = 14;

    public static String extractDroneIdFromNetwork(Socket droneSocket) throws Exception {
		return new String( NetworkFormatter.readNetworkMessage( droneSocket.getInputStream()));
    }

    public static DroneInfo fromNetworkToDroneInfo(InputStream streamIn) throws Exception {
		byte[] result = NetworkFormatter.readNetworkMessage(streamIn);
		final ProtoData.DroneData droneData = ProtoData.DroneData.parseFrom(result);
		final float speedInKmH = droneData.getSpeed() * 3.6f;

		return new DroneInfo(droneData.getDroneId(), droneData.getLatitude(), droneData.getLongitude(), speedInKmH,
				                droneData.getAltitude(), droneData.getVoltage(), droneData.getState());
    }

	public static String fromNetworkMessageToDroneConsoleLog(InputStream clientLogInputStream) throws Exception
	{
		byte[] result = NetworkFormatter.readNetworkMessage(clientLogInputStream);
		return result.toString();
	}

    public static byte[] toNetworkMessage(List<DataPoint> dataPoints) {

		ProtoData.MissionData.Builder missionData = ProtoData.MissionData.newBuilder();

		for (DataPoint point : dataPoints) {
			missionData.addPoint( ProtoData.DataPoint.newBuilder()
					                                 .setLatitude(point.getLat())
					                                 .setLongitude(point.getLng())
					                                 .setSpeed(point.getSpeed())
					                                 .setAltitude(point.getHeight())
					                                 .setAction(point.getAction())
					                                 .build());
		}

		byte[] missionDataArr = ProtoData.Command.newBuilder().setCode(START_MISSION_CODE)
		                                         .setPayload( missionData.build().toByteString())
		                                         .build().toByteArray();

		return NetworkFormatter.createNetworkMessage(missionDataArr);
    }
    
    public static byte[] toNetworkMessage(int commandCode) {
		byte[] command = ProtoData.Command.newBuilder().setCode(commandCode).build().toByteArray();
		return NetworkFormatter.createNetworkMessage(command);
    }
    
}

  
