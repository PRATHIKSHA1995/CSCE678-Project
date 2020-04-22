python -m pip install --user boto3
sudo yum install git -y
git clone https://github.com/PRATHIKSHA1995/CSCE678-Project.git
cd CSCE678-Project
sudo python3.6 -m pip install --upgrade pip
pip3 install virtualenv
cd flaskProject
python3 -m venv cs678
source cs678/bin/activate
pip3 install Flask
pip3 install boto3
pip3 install bokeh
export FLASK_APP=analytics
flask run --host=0.0.0.0