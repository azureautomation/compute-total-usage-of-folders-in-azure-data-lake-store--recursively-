Compute total usage of folders in Azure Data Lake Store (recursively)
=====================================================================

            

Computes the total space used by all sub-directories of an arbitrary directory of an Azure Data Lake Store (by default, the root directory), and upload a files to the Data Lake Store containing the total space used for each sub-directory.


The process involves one API call for each directory at every level of the hierarchy, therefore it can be relatively time-consuming. Processing is parallelized over the sub-directories (by default over 5 threads).


 

 

        
    
TechNet gallery is retiring! This script was migrated from TechNet script center to GitHub by Microsoft Azure Automation product group. All the Script Center fields like Rating, RatingCount and DownloadCount have been carried over to Github as-is for the migrated scripts only. Note : The Script Center fields will not be applicable for the new repositories created in Github & hence those fields will not show up for new Github repositories.
