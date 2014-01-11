package org.lib.sharding.repository;

import com.google.common.base.Objects;
import org.lib.sharding.domain.Node;

import java.io.Serializable;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Objects.toStringHelper;

public class NodeInfo implements Serializable {
	private static final long serialVersionUID = 3962506218211361551l;

	private Node node;
	private long lastUpdateTime;

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(long lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		if (null == object) {
			return false;
		}

		if (!(object instanceof NodeInfo)) {
			return false;
		}

		NodeInfo o = (NodeInfo) object;

		return equal(node, o.node);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(node);
	}

	@Override
	public String toString() {
		return toStringHelper(this)
			.addValue(node)
			.addValue(lastUpdateTime)
			.toString();
	}
}
