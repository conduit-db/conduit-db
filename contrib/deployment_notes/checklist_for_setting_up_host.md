## Production Server Configuration Steps
This is in case I need to redo the server configuration
from scratch I will at least have a checklist to get
through it relatively efficiently.

- ubuntu server 21.04 - required because AMD 5850X need recent linux kernels to unlock all features
- SSH - automatically installed if selected in the initial ubuntu installation
- git ssh -> follow online instructions here: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent
- snap install docker (do not give it root elevation!) - the inconvenience of always needing to use "sudo" is well worth it
- sudo apt install lm-sensors && sudo sensors-detect
- sudo dpkg-reconfigure console-setup -> increase font size
- install mariadb: follow https://www.digitalocean.com/community/tutorials/how-to-install-mariadb-on-ubuntu-20-04
- Then create the conduitdb database and user with GRANT ALL ON... for the user account
- Add ++ swap space (64GB or more)
- install bitcoin-sv node and setup systemd unit for it
- clone conduit-db repo and install python dependencies
    - note: on linux grpcio needs to be installed from source!
            otherwise multiprocessing -> Segfaults
    - note: install uvloop on linux for better concurrency
    - note: currently you will need to manually delete the headers.mmap and block_headers.mmap
            files to do a full reset for some reason...
- setup conduit-raw and conduit-index as systemd units
- setup JBOD for 2 x 2TB SSDs and sort any other RAID / ZFS type things...
