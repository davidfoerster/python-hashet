#!/bin/bash
set -eu -o pipefail
exec <&-
scriptdir="${0%/*}"; [ "$scriptdir" != "$0" ] || scriptdir='.'
declare -a p=( python3 "$scriptdir/hashset.py" --external-encoding=utf-8 )
h="${1:-"${TMPDIR:-/tmp}/hashset.data"}" #" Work-around for Sublime Text

if [ $# -ge 2 ]; then
	shift
else
	set -- foo ba{r,z}
fi


printf '%s\n' "$@" |
"${p[@]}" --internal-encoding=iso-8859-15 --index-int-size=4 \
	--item-int-size=4 --load-factor=3/4 --hash=xx_64 --pickler=string \
	--build - "$h" &&
od -Ax -tx1z -v < "$h" ||
exit

declare -i rv=0
printf '\n%s:\n' 'Dump'
"${p[@]}" --dump "$h" ||
	rv=$(($rv | $?))

printf '\n%s:\n' 'Probe (command-line)'
"${p[@]}" --probe "$h" "$@" ||
	rv=$(($rv | $?))

printf '\n%s:\n' 'Probe (stdin)'
printf '%s\n' "$@" | tac | "${p[@]}" --probe "$h" ||
	rv=$(($rv | $?))

exit "$rv"
