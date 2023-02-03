# AAS with S3

This repository contains the implementation of Asset Administration Shell (AAS) with StorageGrid (SG).

## Prerequisites
 - Install TortoiseGit-2.13.0.1.

 - Install jdk-11.0.17.8.

 - Install Eclipse (4.25.0).

 - Install postgresql 13.8 (Run as administrator)

### S3 Browser

To upload csv files to S3, we use S3 browser as it provides a user-friendly GUI and a faster upload speed.

To begin, using the S3 browser, upload the bridge data from local disk to S3 bucket by clicking “Upload” and following the screenshots below:
 
![image](https://user-images.githubusercontent.com/32246811/214176666-e16a92ce-b74d-4fd3-b2e6-ec06f18c87c3.png)
 
 Then, locate the file:
 
![image](https://user-images.githubusercontent.com/32246811/214176736-9f2f3ac3-fe86-4ee8-9433-a56062b940a4.png)

![image](https://user-images.githubusercontent.com/32246811/214176758-cccde9ea-9a80-423f-9720-9cb3a27d9cdf.png)


### Eclipse IDE


Clone the Git repo to your local directory. 
Open it using Eclipse IDE with Java 11.0.17.8.
Afterwards, navigate to Basyx.components.AASServer/src/test/java/s3/AASWithSG.java.



Set AAS_BUCKET_NAME and SUBMODEL_BUCKET_NAME.

 ![image](https://user-images.githubusercontent.com/32246811/214176792-dc38fbe8-c34a-4380-965a-79b07b4e05cf.png)


Put S3 Configuration values within “”.

 ![image](https://user-images.githubusercontent.com/32246811/214176823-0afb47c1-932a-42d7-aaac-33c285eb650f.png)



Set submodelIdentifier1 (bridge ID)

 ![image](https://user-images.githubusercontent.com/32246811/214176837-73b75508-b811-4f29-91c6-5e4c61f73c50.png)



Set filename (name of the file uploaded through S3 browser to the bucket).

![image](https://user-images.githubusercontent.com/32246811/214176852-78c23a72-25a2-45cf-990b-c95c81a8974e.png)
 
 
![image](https://user-images.githubusercontent.com/32246811/214176862-1c248428-5b96-46f0-bea8-1755f08fdc54.png)

 


After executing the code, we can check if the data is uploaded as desired or not by downloading the bridge json from NetApp S3 Console in web browser.

![image](https://user-images.githubusercontent.com/32246811/214176884-27077a14-5f7e-4f6a-9128-f78dbe6ebce0.png)
 

After downloading, we can check if the values are correct or not using NotePad

 ![image](https://user-images.githubusercontent.com/32246811/214176899-25660f8f-7394-4e7c-b1ce-c76c19443bdc.png)


