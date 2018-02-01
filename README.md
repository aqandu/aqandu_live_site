# aqandu
These are instructions for setting up the Python Virtual Environment and frontend of AQandU on a mac. 

The instructions are inspired by this guide: 
  https://medium.com/@henriquebastos/the-definitive-guide-to-setup-my-python-workspace-628d68552e14
  
***Setting Up Virtual Environment***  
First, make sure you have homebrew installed. To install, simply paste this into your terminal prompt:
  /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

Next, paste the following into your terminal:
  1. brew install pyenv
  2. brew install pyenv-virtualenv
  3. brew install pyenv-virtualenvwrapper

# All virtualenvs will be on...

  4. mkdir ~/.ve 
  
# All projects will be on...

  5. mkdir ~/workspace
  
The last two will intialize your directories, and help you keep your code and your virtual environments seperate. 
This is so that the code is available in all sessions without needing to activate the virtualenv.

Open your ~/.bashrc file. You can do this using vi or vim like so:

  vim ~/.bashrc 

Paste the following into your file:

  export WORKON_HOME=~/.ve
  export PROJECT_HOME=~/workspace
  eval "$(pyenv init -)"
  #pyenv virtualenvwrapper_lazy
  
Save and exit the bashrc file, and end your terminal session. Open a new terminal window, then paste this into the prompt:

  pyenv install 3.6.2 #This installs version 3.6.2
  
Next, paste this:

  pyenv global 3.6.2 #this establishes path priority, for if you add other virtualenvs later
  
Now uncomment the line #pyenv virtualenvwrapper_lazy on your ~/.bashrc and restart the terminal, exiting and closing its window and opening a new one.

***You've set up the Python Virtual Environment, now let's set up the frontend***
Set up the the project like so:

  mkproject aqandu #I named the project aqandu, you can name it whatever you want. 
  
Now, if you want to work on your project, do this:

  workon aqandu
  
You also need to have aqandu on your computer.
The easiest way to do this is to clone the repo from git:

  git clone https://github.com/visdesignlab/aqandu.git
  
There are going to be dependencies missing. To install these, the easiest way is to run main.py:

  python main.py
  
Now, for each dependency do the following:

  pip install <name of dependency>
  
Until you reach the config error. Pascal can send you that, as it's not on github. Just copy/paste it into your aqandu file system, same place as your main.py file.
Run main.py again until you reach get a message that tells you that the "Debugger is Active!". Copy and paste the http into your browser. Ta Dah! You should see the AQandU website. 


