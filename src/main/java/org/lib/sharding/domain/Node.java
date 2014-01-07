package org.lib.sharding.domain;

import java.io.Serializable;

public interface Node extends Serializable {
	public long getId();
	public void setId(long id);
	public String getUrl();
	public void setUrl(String url);
}
