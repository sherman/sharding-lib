package org.lib.sharding.domain;

import com.google.common.base.Objects;

import static com.google.common.base.Objects.equal;

public class ServerNode implements Node {
	protected long id;
	protected String url;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		if (null == object) {
			return false;
		}

		if (!(object instanceof Node)) {
			return false;
		}

		Node o = (Node) object;

		return equal(getId(), o.getId()) && equal(getUrl(), o.getUrl());
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id, url);
	}


	@Override
	public String toString() {
		return Objects.toStringHelper(this)
			.addValue(id)
			.addValue(url)
			.toString();
	}
}
