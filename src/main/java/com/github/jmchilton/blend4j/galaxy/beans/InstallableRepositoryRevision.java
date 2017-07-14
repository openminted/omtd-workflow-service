package com.github.jmchilton.blend4j.galaxy.beans;

import com.github.jmchilton.blend4j.toolshed.beans.RepositoryRevision;
import org.codehaus.jackson.annotate.JsonProperty;

public class InstallableRepositoryRevision {
  @JsonProperty(value="changeset_revision")
  protected String changsetRevision;
  protected String name;
  protected String owner;
  @JsonProperty(value="tool_shed_url")
  protected String toolShedUrl = "https://toolshed.g2.bx.psu.edu/";

  public InstallableRepositoryRevision() {
  }
  
  public InstallableRepositoryRevision(final String toolShedUrl, 
                                       final RepositoryRevision repositoryRevision) {
    this.toolShedUrl = toolShedUrl;
    this.name = repositoryRevision.getName();
    this.owner = repositoryRevision.getOwner();
    this.changsetRevision = repositoryRevision.getRevision();
  }
  
  public InstallableRepositoryRevision(final InstallableRepositoryRevision revision) {
    this.toolShedUrl = revision.getToolShedUrl();
    this.changsetRevision = revision.getChangsetRevision();
    this.owner = revision.getOwner();
    this.name = revision.getName();    
  }
  
  public String getChangsetRevision() {
    return changsetRevision;
  }

  public String getName() {
    return name;
  }

  public String getOwner() {
    return owner;
  }

  public String getToolShedUrl() {
    return toolShedUrl;
  }

  public void setChangsetRevision(String changsetRevision) {
    this.changsetRevision = changsetRevision;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public void setToolShedUrl(String toolShedUrl) {
    this.toolShedUrl = toolShedUrl;
  }

}
