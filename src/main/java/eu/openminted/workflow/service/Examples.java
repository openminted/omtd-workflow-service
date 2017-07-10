package eu.openminted.workflow.service;

public class Examples {

	public static void main(String args[]){
		String galaxyInstanceUrl = "http://snf-754063.vm.okeanos.grnet.gr";
		String galaxyApiKey = "<...>";
		
		Galaxy galaxy = new Galaxy(galaxyInstanceUrl, galaxyApiKey);
		
		//galaxy.runWorkflow("/home/ilsp/Desktop/DG/OMTD/omtd-simple-workflows/testInput/", "DGTest1");
		//galaxy.runWorkflow("C:/Users/galanisd/Desktop/smallPDFs/", "DGTest1", "C:/Users/galanisd/Desktop/smallPDFsOut/");		
		galaxy.runWorkflow("C:/Users/galanisd/Desktop/Dimitris/EclipseWorkspaces/ILSPMars/omtd-simple-workflows/testInput/", "DGTest1", "C:/Users/galanisd/Desktop/smallPDFsOut/");
				
	}
}
