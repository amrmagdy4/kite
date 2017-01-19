package edu.umn.cs.kite.querying.metadata;

import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by amr_000 on 12/28/2016.
 */
public class CommandMetadataEntry implements MetadataEntry {
    private String command;
    private List<Object> arguments;

    public CommandMetadataEntry(String command, String arg1) {
        this.command = command;
        arguments = new ArrayList<>();
        arguments.add(arg1);
    }

    public CommandMetadataEntry(String command, String arg1, String arg2) {
        this(command, arg1);
        arguments.add(arg2);
    }

    public CommandMetadataEntry(String command, String arg1, String arg2, String
            arg3) {
        this(command, arg1, arg2);
        arguments.add(arg3);
    }

    @Override public boolean isStreamEntry() {
        return false;
    }
    @Override public boolean isIndexEntry() {
        return false;
    }
    @Override public boolean isCommandEntry() {
        return true;
    }
    @Override public boolean isQueryEntry() { return false; }

    @Override
    public void serialize(ByteStream byteStream) {
        //commands are not serialized
    }

    public String getCommand() {
        return command;
    }

    public Object getArg(int i) {
        return i < arguments.size()? arguments.get(i):null;
    }

    public String getArgsString() {
        String str = "";
        for(int i = 0; i < arguments.size()-1; ++i)
            str += arguments.get(i).toString()+",";
        if(arguments.size() > 0)
            str += arguments.get(arguments.size()-1);
        return str;
    }
}
