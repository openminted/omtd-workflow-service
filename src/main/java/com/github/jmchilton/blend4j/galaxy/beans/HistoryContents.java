package com.github.jmchilton.blend4j.galaxy.beans;

import com.github.jmchilton.blend4j.util.Objects;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class HistoryContents extends GalaxyObject {
  private String name;
  private String type = "file";
  private boolean deleted = false;
  private boolean purged = false;
  private int hid;
  private String historyContentType = "dataset";
  private String state;
  private boolean visible = false;

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  @Deprecated
  public String getType() {
    return type;
  }

  public void setType(final String type) {
    this.type = type;
  }
  
  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public boolean isPurged() {
    return purged;
  }

  public void setPurged(boolean purged) {
    this.purged = purged;
  }

  public int getHid() {
    return hid;
  }

  public void setHid(int hid) {
    this.hid = hid;
  }

  public String getHistoryContentType() {
    return historyContentType;
  }

  @JsonProperty("history_content_type")
  public void setHistoryContentType(String historyContentType) {
    this.historyContentType = historyContentType;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public boolean isVisible() {
    return visible;
  }

  public void setVisible(boolean visible) {
    this.visible = visible;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(deleted, hid, historyContentType, name, purged, state, type);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HistoryContents) {
      HistoryContents other = (HistoryContents)obj;
      
      return Objects.equal(deleted, other.deleted) &&
          Objects.equal(hid, other.hid) &&
          Objects.equal(historyContentType, other.historyContentType) &&
          Objects.equal(name, other.name) && 
          Objects.equal(purged, other.purged) &&
          Objects.equal(state, other.state) &&
          Objects.equal(visible, other.visible) &&
          Objects.equal(type, other.type);
    }
    
    return false;
  }
}
