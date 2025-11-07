# latest

This is a very simple library that runs an http server which serves
the "latest" file from each of a collection of directories you configure.

What "latest" means for each directory is user-configurable. By default, it's the file
in the directory with the latest modification time. When any of the directories change,
it recomputes all the latests.

> [!NOTE]
> Dotfiles are always ignored! This cannot be overridden.


