sudo yum install git -y
git clone https://github.com/PRATHIKSHA1995/CSCE678-Project.git
cd CSCE678-Project
cd flaskProject
pip install Flask
pip install boto3
pip install bokeh
export FLASK_APP=analytics
python3 -m flask run --host=0.0.0.0