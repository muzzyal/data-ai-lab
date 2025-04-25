alias ll='ls -alF --color=auto'
alias ls='ls --color=auto'
parse_git_branch() {
     git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/(\1)/'
}
export PS1="\[\033[01;32m\]\u:\033:\[\033[01;34m\]\w\[\033[00m\] \[\e[91m\]$(parse_git_branch)\[\e[00m\]]]]]]$ "
