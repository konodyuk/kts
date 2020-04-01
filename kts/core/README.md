# Design Notes

This document contains notes on internal design which may not be obvious from source code and should be considered in case of any refactoring.

## Caching Policy

<table style="border-collapse: collapse; border: none; border-spacing: 0px;">
	<tr>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt;">
		</td>
		<td colspan="2" style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: center;">
            parallel={true, false}
        </td>
		<td colspan="2" style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: center;">
            parallel=false
		</td>
	</tr>
	<tr>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: center;">
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: center;">
			cached (first call)
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: center;">
			cached (second+ call)
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: center;">
            not cached (first call)
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            not cached (second+ call)
		</td>
	</tr>
	<tr>
		<td rowspan="3" style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; transform: rotate(-90deg); text-align: center;">
            train
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_frame &rarr; [ret], [1f]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            [1f] &rarr; [ret]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_frame &rarr; [ret]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_frame &rarr; [ret]
		</td>
	</tr>
	<tr>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_state &rarr; [1s]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_state &rarr; [3s]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_state &nrarr; (dropped at worker level)
		</td>
	</tr>
	<tr>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            stats &rarr; [1stats]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            stats &rarr; [3stats]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            stats &rarr; [3stats] (ignored at RunCache level)
		</td>
	</tr>
	<tr>
		<td rowspan="3" style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; transform: rotate(-90deg); text-align: center;">
        test
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_frame &rarr; [ret], [2f]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            [2f] &rarr; [ret]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_frame &rarr; [ret]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            res_frame &rarr; [ret]
		</td>
	</tr>
	<tr>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            state &larr; [1s]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            state &larr; [3s]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            state &larr; [3s]
		</td>
	</tr>
	<tr>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            stats &rarr; [2stats]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            stats &rarr; [4stats]
		</td>
		<td style="border: 1px solid rgb(0, 0, 0); padding-right: 3pt; padding-left: 3pt; text-align: left;">
            stats &rarr; [4stats] (ignored at RunCache level)
		</td>
	</tr>
</table>