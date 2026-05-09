curl -L https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs  -o $HOME/minio-binaries/mc
chmod +x $HOME/minio-binaries/mc
export PATH=$PATH:$HOME/minio-binaries/
#echo export PATH=\$PATH:\$HOME/minio-binaries/ >> ~/.bashrc
mc --version
