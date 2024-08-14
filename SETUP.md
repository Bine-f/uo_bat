# SETUP Analysis environment

This markdown file is used to help the user setup the analysis environment.
Be sure to run the commands from the correct directory. The commands are
listed in the order they should be run. Root directory is the directory of the ```Analysis_template``` folder.

## Create a virtual environment for the project

- Create a virtual environment called ```venv_name```

If you don't have ```venv``` installed, type:

        pip3 install virtualenv

The command below will create a new folder called ```venv_name```

        python -m venv <venv_name>
    
NOTE: the virtual environment name can be anything (like ```banana```).
However, it is common practice to use just ```venv``` or could be something with regards to your analysis. This is handy when copying/pasting code snippets.

Hence, use the following command:

        python3 -m venv venv
    
**IMPORTANT**: if you have used a different name for the virtual environment, you will have to change the name in the ```.gitignore``` file.
---

## Activate the new virtual environment ```venv```

If using the default command prompt (```cmd```), type:

        .\venv\Scripts\activate

If using Windows PowerShell (```PS```), type:

        .\venv\Scripts\Activate.ps1

**IMPORTANT**: if no changes seem to occur, open a new terminal prompt to activate the environment.

The active path should now be preceded by ```(venv)``` or ```(banana)``` if a specific venv name was chosen.

If you are using macOS, you can use the following command:

        source venv/bin/activate

---

## Install base_libraries.txt

    Be sure to install the requirements.txt file. it is located in the env_setup folder of the project.

        pip3 install -r ./requirements.txt

    If you need any more libraries, add them to the requirements.txt file and run the command again. PLEASE AVOID INSTALLING LIBRARIES MANUALLY as this might cause the requirements.txt file to be outdated - when somebody behind you will be running the repositoriy he will get unnecessary errors.

---

## Install a Python Library

If installing a single package, type:

    pip3 install package_name

If installing more than one package, type:

    pip3 install package_name_1 package_name_2

If installing a specific version of a package, type:

    pip3 install package_name=1.6

Be sure to add the package to the requirements.txt file.

---

## Deactivate the virtual environment

    deactivate

---

## Taking care of the input data and results data

Be sure to not incluce the input data and results data in the repository if it is too big. This will make the repository too big and will slow down the repository. Instead, use the following steps:

- Create a folder called ```input_data``` in the root directory of the project
- Create a folder called ```results_data``` in the root directory of the project
- Add the ```input_data``` and ```results_data``` folders to the ```.gitignore``` file (Already done for you)

If you really need to include the data in the repository, you can use ```lfs```. This is a bit more complicated and is not recommended. If you want to use ```lfs```, you can find more information [here](https://git-lfs.github.com/).

---
## Fill the DOCUMENTATION DATA

The README.md file is used to display the project information. Template is already made for you.
The data that you have to fill in are:

- Project name
- Project author
- Project description
- Input data
- Output data

---

