package com.jtljia.pump.canal.parse.driver.mysql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jtljia.pump.canal.parse.driver.mysql.packets.HeaderPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.client.QueryCommandPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.ErrorPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.OKPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.ResultSetHeaderPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.RowDataPacket;
import com.jtljia.pump.canal.parse.driver.mysql.utils.PacketManager;

/**
 * 默认输出的数据编码为UTF-8，如有需要请正确转码
 * @author wujie
 */
public class MysqlQueryExecutor {
	
	private static final Logger logger = LoggerFactory.getLogger(MysqlQueryExecutor.class);
	
    private MySQLConnector connector;

    public MysqlQueryExecutor(MySQLConnector connector){
        this.connector = connector;
    }
    
	public void connect() throws IOException {
        connector.connect();
    }

    public void reconnect() throws IOException {
        connector.reconnect();
    }

    public void disconnect() throws IOException {
        connector.disconnect();
    }

    public boolean isConnected() {
        return connector.isConnected();
    }
        
    public MysqlQueryExecutor fork() {
    	return new MysqlQueryExecutor(connector.fork());
    }
    
    /**
     * (Result Set Header Packet) the number of columns <br>
     * (Field Packets) column descriptors <br>
     * (EOF Packet) marker: end of Field Packets <br>
     * (Row Data Packets) row contents <br>
     * (EOF Packet) marker: end of Data Packets
     * 
     * @param queryString
     * @return
     * @throws IOException
     */
    public ResultSetPacket query(String queryString) throws IOException {
    	if (!connector.isConnected()) {
    		connector.connect();
        }
    	
        QueryCommandPacket cmd = new QueryCommandPacket();
        cmd.setQueryString(queryString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.write(connector.getChannel(), bodyBytes);
        byte[] body = readNextPacket();

        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.fromBytes(body);
            throw new IOException(packet + "\n with command: " + queryString);
        }

        ResultSetHeaderPacket rsHeader = new ResultSetHeaderPacket();
        rsHeader.fromBytes(body);

        List<FieldPacket> fields = new ArrayList<FieldPacket>();
        for (int i = 0; i < rsHeader.getColumnCount(); i++) {
            FieldPacket fp = new FieldPacket();
            fp.fromBytes(readNextPacket());
            fields.add(fp);
        }

        readEofPacket();

        List<RowDataPacket> rowData = new ArrayList<RowDataPacket>();
        while (true) {
            body = readNextPacket();
            if (body[0] == -2) {
                break;
            }
            RowDataPacket rowDataPacket = new RowDataPacket();
            rowDataPacket.fromBytes(body);
            rowData.add(rowDataPacket);
        }

        ResultSetPacket resultSet = new ResultSetPacket();
        resultSet.getFieldDescriptors().addAll(fields);
        for (RowDataPacket r : rowData) {
            resultSet.getFieldValues().addAll(r.getColumns());
        }
        resultSet.setSourceAddress(connector.getChannel().socket().getRemoteSocketAddress());

        return resultSet;
    }
    
    public OKPacket update(String updateString) throws IOException {
        QueryCommandPacket cmd = new QueryCommandPacket();
        cmd.setQueryString(updateString);
        byte[] bodyBytes = cmd.toBytes();
        PacketManager.write(connector.getChannel(), bodyBytes);

        logger.debug("read update result...");
        byte[] body = PacketManager.readBytes(connector.getChannel(), PacketManager.readHeader(connector.getChannel(), 4).getPacketBodyLength());
        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.fromBytes(body);
            throw new IOException(packet + "\n with command: " + updateString);
        }

        OKPacket packet = new OKPacket();
        packet.fromBytes(body);
        return packet;
    }
    
    private void readEofPacket() throws IOException {
        byte[] eofBody = readNextPacket();
        if (eofBody[0] != -2) {
            throw new IOException("EOF Packet is expected, but packet with field_count=" + eofBody[0] + " is found.");
        }
    }

    protected byte[] readNextPacket() throws IOException {
        HeaderPacket h = PacketManager.readHeader(connector.getChannel(), 4);
        return PacketManager.readBytes(connector.getChannel(), h.getPacketBodyLength());
    }
}
