# AAS with SG

This repository contains the implementation of Asset Administration Shell (AAS) with StorageGrid (SG).

## Prerequisites
 - Install TortoiseGit-2.13.0.1. <img src="https://user-images.githubusercontent.com/32246811/216552329-3a2cdc03-7f0e-4fd2-930f-690b19c0e203.png" alt="tortgit" width="25"/>

 - Install jdk-11.0.17.8. <img src="https://user-images.githubusercontent.com/32246811/216548182-4ca8da82-96e5-4144-bf08-391bef460d08.png" alt="javadsk" width="25"/>

 - Install Eclipse (4.25.0). <img src="https://user-images.githubusercontent.com/32246811/216552744-672f838f-2823-45ff-8e4c-df349864bb64.png"  alt="eclipse" width="25"/>

 - Install postgresql 13.8 (Run as administrator). <img src="https://user-images.githubusercontent.com/32246811/216552772-8cf7a495-8f8e-487e-9681-d820f16e67d7.png"  alt="psql" width="25"/>

### S3 Browser

To upload csv files to S3, we use S3 browser as it provides a user-friendly GUI and a faster upload speed.

To begin, using the S3 browser, upload the bridge data from local disk to S3 bucket by clicking “Upload” and following the screenshots below:
 
![image](https://user-images.githubusercontent.com/32246811/214176666-e16a92ce-b74d-4fd3-b2e6-ec06f18c87c3.png)
 
Then, locate the file:
 
![image](https://user-images.githubusercontent.com/32246811/214176736-9f2f3ac3-fe86-4ee8-9433-a56062b940a4.png)

![image](https://user-images.githubusercontent.com/32246811/214176758-cccde9ea-9a80-423f-9720-9cb3a27d9cdf.png)

### Cloning the remote repository to local machine using TortoiseGit

Then we use TortoiseGit to clone the repository.
Set URL as "https://github.com/hassaanwasim9/AASwithSG.git" and directory as the desired user folder.
Set branch as "main" and click OK.

![image](https://user-images.githubusercontent.com/32246811/216554783-d1bd6ecb-07d6-4054-b21d-c64a8243921b.png)


### Eclipse IDE

Once data (csv file) is uploaded to S3 and our local repository is ready to use, we switch to Eclipse IDE to upload the metadata of the file as well.

 - After opening Eclipse IDE, navigate to "*Basyx.components.AASServer/src/test/java/s3/AASWithSG.java*".
![image](https://user-images.githubusercontent.com/32246811/216555915-b5377a17-a248-4b61-b7e8-4e3100e80897.png)

Path to AASWithSG.java in the current remote repository is "*AASwithSG/basyx.components/basyx.components.docker/basyx.components.AASServer/src/test/java/org/eclipse/basyx/regression/AASServer/s3/AASWithSG.java*"

 - After opening the file in Eclipse, Set AAS_BUCKET_NAME and SUBMODEL_BUCKET_NAME.

 ![image](https://user-images.githubusercontent.com/32246811/214176792-dc38fbe8-c34a-4380-965a-79b07b4e05cf.png)


 - Then, put S3 Configuration values within “”.

 ![image](https://user-images.githubusercontent.com/32246811/214176823-0afb47c1-932a-42d7-aaac-33c285eb650f.png)

 - Afterwards, set submodelIdentifier1 (bridge ID)

 ![image](https://user-images.githubusercontent.com/32246811/214176837-73b75508-b811-4f29-91c6-5e4c61f73c50.png)


 - Finally, set filename (name of the file uploaded through S3 browser to the bucket).

![image](https://user-images.githubusercontent.com/32246811/214176852-78c23a72-25a2-45cf-990b-c95c81a8974e.png)
 
 
![image](https://user-images.githubusercontent.com/32246811/214176862-1c248428-5b96-46f0-bea8-1755f08fdc54.png)

 Data Loaded! <img src="https://user-images.githubusercontent.com/32246811/216557963-fe222ff6-6813-4ed6-a016-ff746ad9ec44.jpg" alt="smiley" width=25>


After executing the code, we can check if the data is uploaded as desired or not by downloading the bridge json from NetApp S3 Console in web browser.

![image](https://user-images.githubusercontent.com/32246811/214176884-27077a14-5f7e-4f6a-9128-f78dbe6ebce0.png)
 

After downloading the, we can check if the values are correct or not using NotePad

 ![image](https://user-images.githubusercontent.com/32246811/214176899-25660f8f-7394-4e7c-b1ce-c76c19443bdc.png)


