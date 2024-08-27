package com.eshore.otter.canal.parse.driver.mysql.packets.server;

import java.io.IOException;

import com.eshore.otter.canal.parse.driver.mysql.packets.CommandPacket;
import com.eshore.otter.canal.parse.driver.mysql.utils.ByteHelper;

public class AuthSwitchRequestPacket extends CommandPacket {

    public int    status;
    public String authName;
    public byte[] authData;

    public void fromBytes(byte[] data) {
        int index = 0;
        // 1. read status
        status = data[index];
        index += 1;
        byte[] authName = com.eshore.otter.canal.parse.driver.mysql.utils.ByteHelper.readNullTerminatedBytes(data, index);
        this.authName = new String(authName);
        index += authName.length + 1;
        authData = ByteHelper.readNullTerminatedBytes(data, index);
    }

    public byte[] toBytes() throws IOException {
        return null;
    }

}
