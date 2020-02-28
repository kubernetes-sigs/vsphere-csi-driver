#!/bin/bash

set -e

function cleanup() {
   printf "\n\tSTEP 5: Cleaning up\n\n"
   cd "${dir}"
   if [[ -d mirrors_internal_vsphere-csi-driver ]]; then
      rm -rf mirrors_internal_vsphere-csi-driver
   fi 

   if [[ ${account} != "" ]]; then 
      ssh -T "${account}" << EOF 
         if [[ -d core-build ]]; then 
            rm -rf core-build
         fi 
EOF
   fi 

}

function update_mirrors () {
   printf "\n\tSTEP 1: Updating mirrors_internal_vsphere-csi-driver\n\n"

   if [[ -d mirrors_internal_vsphere-csi-driver ]]; then
      rm -rf mirrors_internal_vsphere-csi-driver
   fi
   git clone git@gitlab.eng.vmware.com:core-build/mirrors_internal_vsphere-csi-driver.git

   cd mirrors_internal_vsphere-csi-driver

   git remote add upstream https://gitlab.eng.vmware.com/hatchway/vsphere-csi-driver

	git checkout cns_csi

	printf "\n\tSTEP 1(a): cherry-picking from master to cns_csi\n\n"

	git cherry-pick "${START_COMMIT}".."${END_COMMIT}"

	printf "\n\tSTEP 1(b): Running go mod vendor to update dependencies.\n\n"

	go mod vendor

	printf "\n\tSTEP 1(c): Committing vendored dependencies, if they exist\n\n"
	if [[ $(git status --porcelain) ]]; then
      git add .
      git commit -m "Updating vendor with latest dependencies."
   fi

   printf "\n\tSTEP 1(d): Trying to build mirrors-internal-vsphere_csi_driver.\n\n"
   
   make

   printf "\n\tSTEP 1(e): Successfully built mirrors-internal-vsphere_csi_driver!\n\n"

   git push
	
   git tag v0.0.1.alpha+vmware."${INTERNAL_VSPHERE_TAG}"

   git push --tags

   cd ..

}

function update_cayman_vsphere_csi-driver() {
   printf "\n\tSTEP 2: Updating cayman_vsphere_csi-driver\n\n"

   until [[ ${success} == 'yes' ]]; do 
      read -r -p "Enter your dbc hostname (eg. pa-dbc1103.eng.vmware.com): " account
   if ! (ssh "${account}") << EOF
EOF
   then
      printf "There was an error. Please enter a valid dbc hostname.\n"
   else 
      success="yes"
   fi
   done

   version="v0.0.1.alpha+vmware"
   read -r -p "Do you want to alter the version of vsphere-csi-driver? Default: v0.0.1.alpha+vmware (yes/no) ": alter;

   if [[ ${alter} == "yes" ]]; then
      read -r -p "Enter new version: " version;
   fi

   printf "\n\tSTEP 2(a): Logging into dbc account %s\n\n" "${account}"

   ssh -T "${account}" << EOF 
      set -e 
      if [[ -d core-build ]]; then 
         rm -rf core-build
      fi 
      mkdir core-build
      cd core-build
       
      printf "\n\tSTEP 2(b): Cloning cayman_vsphere-csi-driver into core-build/cayman_vsphere-csi-driver\n\n" 
      git clone git@gitlab.eng.vmware.com:core-build/cayman_vsphere-csi-driver.git
      cd cayman_vsphere-csi-driver

      printf "\n\tSTEP 2(c): Checking out cns_csi branch\n\n"
      git checkout cns_csi

      OLD_LINE="\$(grep "version" support/gobuild/provenance/cayman_vsphere_csi_driver.yaml)"; 
      PREFIX="     version: \"v0.0.1.alpha+vmware."; 
       
      printf "\n\tSTEP 2(d): Updating support/gobuild/provenance/cayman_vsphere_csi_driver.yaml with tag %s\n\n" "${INTERNAL_VSPHERE_TAG}"
      sed -i "s/^     version.*/     version: \"${version}.${INTERNAL_VSPHERE_TAG}\"/" support/gobuild/provenance/cayman_vsphere_csi_driver.yaml
       
      printf "\n\tSTEP 2(e): Updating support/gobuild/provenance/cayman_vsphere-csi-driver.yaml with tag %s\n\n" "${INTERNAL_VSPHERE_TAG}"
      sed -i "s/^     version.*/     version: \"${version}.${INTERNAL_VSPHERE_TAG}\"/" support/gobuild/provenance/cayman_vsphere-csi-driver.yaml
       
      printf "\n\tSTEP 2(f): Running git submodule\n\n" 
      git submodule update --init --recursive --remote
       
      printf "\n\tSTEP 2(g): Committing changes to cayman_vsphere_csi_driver.\n\n"
      git add .
      git commit -m "Bumping up the tag to ${version}.${INTERNAL_VSPHERE_TAG}"
      NEW_COMMIT=\$(git log --format=%H -n 1)
      printf "\nNew commit id: %s\n\n" "\${NEW_COMMIT}"
       
      /build/apps/bin/gobuild sandbox queue cayman_vsphere_csi_driver --branch cns_csi --no-store-trees --bootstrap "cayman_vsphere_csi_driver=git-eng:core-build/cayman_vsphere-csi-driver.git;%(branch);" --changeset \$NEW_COMMIT --buildtype beta

EOF

   printf "\n\n"

   success=""

   until [[ ${success} == 'yes' || ${success} == 'no' ]]; do 
      read -r -p "Build passed (yes/no): " success;
   done

   if [ "${success}" == 'no' ]; then
      printf "Build failed. Exiting\n"
      exit 1
   else 
      printf "Build passed! Moving on..\n"
   fi

   ssh -T "${account}" << EOF 
      set -e
      cd core-build/cayman_vsphere-csi-driver
      pwd
      printf "\n\tSTEP 2(h): Pushing new commit to cayman_vsphere-csi-driver\n\n"
      git push
EOF
}

function update_vsphere-csi-driver() {
   ssh -T "${account}" << EOF 
      set -e 
      cd core-build/cayman_vsphere-csi-driver
      CAYMAN_VSPHERE_COMMIT=\$(git log --format=%H -n 1)
      cd ..
      printf "\n\tSTEP 3: Updating core-build/vsphere-csi-driver.\n\n" 
       
      printf "\n\tSTEP 3(a): Cloning core-build/vsphere-csi-driver\n\n"    
      git clone git@gitlab.eng.vmware.com:core-build/vsphere-csi-driver.git
      cd vsphere-csi-driver

      printf "\n\tSTEP 3(b): Updating VSPHERE_CSI_DRIVER_CLN commit id in support/gobuild/specs/vsphere_csi_driver.py\n\n"
      sed -i "s/^VSPHERE_CSI_DRIVER_CLN.*/VSPHERE_CSI_DRIVER_CLN = '\${CAYMAN_VSPHERE_COMMIT}'/" support/gobuild/specs/vsphere_csi_driver.py

      printf "\n\tSTEP 3(c): Adding a new file vsphere_csi_driver/vsphere-csi-driver-v0.0.1.alpha+vmware.xx+1 with new tag from cayman_vsphere-csi-driver and removing the old vsphere_csi_driver/vsphere-csi-driver-v0.0.1.alpha+vmware.xx\n\n"

      file=\`find ./vsphere_csi_driver/ -name "vsphere-csi-driver-v0.0.1.alpha+vmware*"\`
      prefix="./vsphere_csi_driver/vsphere-csi-driver-v0.0.1.alpha+vmware."
      oldtag=\${file##"\$prefix"}
      NEW_TAG="\$((oldtag+1))"
      newfile=\$prefix\$NEW_TAG
      mv \$file \$newfile
       
      sed -i "s/^vsphere_csi_driver.*/vsphere_csi_driver=v0\.0\.1\.alpha\+vmware\.${INTERNAL_VSPHERE_TAG}/" \$newfile

      printf "\n\tSTEP 3(d): Committing changes to cayman_vsphere-csi-driver.\n\n"
      git add .
      git commit -m "Update commit id for cayman_vsphere-csi-driver + Added source file for tag \$NEW_TAG"
      git push
      git tag v0.0.1.alpha+vmware.\$NEW_TAG
      git push --tags

      /build/apps/bin/gobuild sandbox queue vsphere_csi_driver --branch vmware-0.0.1.alpha --no-store-trees --bootstrap "vsphere_csi_driver=git-eng:core-build/vsphere-csi-driver.git;%(branch);" --changeset False --buildtype beta

EOF
   printf "\n\n"
   success=""

   until [[ ${success} == 'yes' || ${success} == 'no' ]]; do 
      read -r -p "Build passed (yes/no): " success;
   done

   if [ "${success}" == 'no' ]; then
      printf "Build failed. Exiting\n"
      exit 1
   else 
      printf "Build passed! Moving on..\n\n"
   fi

}

function update_cayman_photon() {
   read -r -p "Enter your username: " username
   printf "\n"
   ssh -T "${account}" << EOF
      set -e
      cd core-build/vsphere-csi-driver
      VSPHERE_CSI_DRIVER_COMMIT=\$(git log --format=%H -n 1)
      cd ..

      printf "\n\tSTEP 4: Updating cayman_photon\n\n"
      printf "\n\tSTEP 4(a): Cloning core-build/cayman_photon\n\n"
      git clone git@gitlab.eng.vmware.com:core-build/cayman_photon.git
      cd cayman_photon
      branch_name="topic/${username}/csi-spec-bump-$(date +%s)"  

      printf "\n\tSTEP 4(b): Creating branch %s\n\n" "\${branch_name}"
      git checkout -b \${branch_name}
     
      printf "\n\tSTEP 4(c): Updating VSPHERE_CSI_DRIVER_CLN in support/gobuild/specs/cayman_photon.py\n\n"
      sed -i "s/^VSPHERE_CSI_DRIVER_CLN.*/VSPHERE_CSI_DRIVER_CLN = '\${VSPHERE_CSI_DRIVER_COMMIT}'/" support/gobuild/specs/cayman_photon.py

      printf "\n\tSTEP 4(d): Committing changes to cayman_photon on branch %s\n\n" "\${branch_name}"
      git add . 
      git commit -s -m "Spec bump for vSphere CSI driver."

      git push --set-upstream origin \${branch_name}

      /build/apps/bin/gobuild sandbox queue cayman_stateless_photon --accept-defaults --branch \${branch_name} --buildtype release --no-changeset
EOF

}

printf "\tWelcome! This script automates the vsphere csi driver spec bump onto cayman photon. Usage:\n\n\t\t./run-spec-bump.sh <commit1> <commit2> <vsphere-tag>\n\n\t<commit1> - commit id (exclusive) from the master branch on core-build/mirrors_internal_vsphere-csi-driver from which you want to begin the cherry-picking.\n\t<commit2> - commit id (inclusive) from the master branch on core-build/mirrors_internal_vsphere-csi-driver upto which you want to run the cherry-picking.\n\t<vsphere-tag> - newest tag on mirrors_internal_vsphere-csi-driver that will be created for this spec bump. Take the highest existing tag number and add 1.\n\nIf this script encounters any errors, it will print the error message and exit. Intermediate failures require the user to triage and perform the spec bump manually."

START_COMMIT=${1:-'FAIL'}
END_COMMIT=${2:-'FAIL'}
INTERNAL_VSPHERE_TAG=${3:-'FAIL'}

if [ "${START_COMMIT}" == 'FAIL' ]; then
   printf "OOPS! Please provide start commit id for cherry pick (exclusive).\n\n"
   exit 1;
fi

if [ "${END_COMMIT}" == 'FAIL' ]; then
   printf "OOPS! Please provide end commit id for cherry pick (inclusive).\n\n"
   exit 1;
fi

if [ "${INTERNAL_VSPHERE_TAG}" == 'FAIL' ]; then
   printf "OOPS! Please provide the new tag for mirrors_internal_vsphere-csi-driver.\n\n"
   exit 1;
fi

dir=$(pwd)
trap cleanup EXIT
update_mirrors
update_cayman_vsphere_csi-driver
update_vsphere-csi-driver
update_cayman_photon

printf "\n\nYou're all set!\nBefore creating a merge request on cayman_photon, verify that the build above is successful.\nIf it is, use the build number to obtain a testbed using the jenkins job: https://wcp.svc.eng.vmware.com/job/dev-integrate-wcp/build?delay=0sec (enter the build number in the OVA_BUILD field).\nRun vsphere-csi-driver e2e tests (Optional, depending on whether or not e2e was run before).\nCreate a Pull request on gitlab to merge changes to cayman_photon (eg. https://gitlab.eng.vmware.com/core-build/cayman_photon/merge_requests/282/commits).\n\n"


 
