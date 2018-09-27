package com.jtljia.pump.canal.protocol.position;

/**
 * 基于mysql/oracle log位置标示
 */
public class ReplicationPosition{

    private SlaveIdentity     identity;
    private EntryPosition     postion;

    public SlaveIdentity getIdentity() {
        return identity;
    }

    public void setIdentity(SlaveIdentity identity) {
        this.identity = identity;
    }

    public EntryPosition getPostion() {
        return postion;
    }

    public void setPostion(EntryPosition postion) {
        this.postion = postion;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identity == null) ? 0 : identity.hashCode());
        result = prime * result + ((postion == null) ? 0 : postion.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ReplicationPosition)) {
            return false;
        }
        ReplicationPosition other = (ReplicationPosition) obj;
        if (identity == null) {
            if (other.identity != null) {
                return false;
            }
        } else if (!identity.equals(other.identity)) {
            return false;
        }
        if (postion == null) {
            if (other.postion != null) {
                return false;
            }
        } else if (!postion.equals(other.postion)) {
            return false;
        }
        return true;
    }

}
