package com.github.jmchilton.blend4j.galaxy.beans.collection.request;

import java.util.LinkedList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A request to create a collection of elements (datasets or other collections) within Galaxy.
 */
public class CollectionDescription {
  
  @JsonProperty("name")
  private String name;
  
  @JsonProperty("type")
  private String type = "dataset_collection";
  
  @JsonProperty("collection_type")
  private String collectionType = "list";
  
  @JsonProperty("element_identifiers")
  private List<AbstractElement> datasetElements
    = new LinkedList<AbstractElement>();
  
  /**
   * Gets the name of this dataset collection.
   * @return  The name of this dataset collection.
   */
  public String getName() {
    return name;
  }
  
  /**
   * Sets the name of this dataset collection.
   * @param name  The name of this dataset collection.
   */
  public void setName(String name) {
    this.name = name;
  }
  
  /**
   * Gets a list of elements within this dataset collection.
   * @return  A list of elements within this dataset collection.
   */
  public List<AbstractElement> getDatasetElements() {
    return datasetElements;
  }
  
  /**
   * Adds an element to this dataset collection.
   * @param datasetElement The dataset element to add.
   */
  public void addDatasetElement(AbstractElement datasetElement) {
    this.datasetElements.add(datasetElement);
  }

  /**
   * Gets the collection type.
   * @return  The collection type.
   */
  public String getCollectionType() {
    return collectionType;
  }

  /**
   * Sets the collection type.
   * @param collectionType  The collection type.
   */
  public void setCollectionType(String collectionType) {
    this.collectionType = collectionType;
  }

  /**
   * Gets the type of this request.
   * @return  The type of this request.
   */
  public String getType() {
    return type;
  }

  /**
   * Sets the type of this request.
   * @param type  The type of this request.
   */
  public void setType(String type) {
    this.type = type;
  }
}
