CSS_STYLE = """
.kts .wrapper {{
  display: inline-flex;
  flex-direction: column;
  background-color: {first};
  padding: 10px;
  border-radius: 20px;
}}
.kts .wrapper-border {{
  border: 0px solid {second};
}}
.kts .pool {{
  display: flex;
  flex-wrap: wrap;
  background-color: {second};
  padding: 5px;
  border-radius: 20px;
  margin: 5px;
}}
.kts .field {{
  text-align: left;
  border-radius: 15px;
  padding: 5px 15px;
  margin: 5px;
  display: inline-block;
}}
.kts .field-bg {{
  background-color: {second};
}}
.kts .field-bold {{
  font-weight: bold;
}}
.kts .field-third {{
  color: {third};
}}
.kts .field-accent {{
  color: {accent};
}}
.kts .field-bg:hover {{
  background-color: {fourth};
}}
.kts .annotation {{
  text-align: left;
  margin-left: 20px;
  margin-bottom: -5px;
  display: inline-block;
  color: {third};
}}
.kts .title {{
  text-align: center;
  display: inline-block;
  font-weight: bold;
  color: {third};
}}
.kts .code {{
  background-color: {second};
  text-align: left;
  border-radius: 15px;
  padding: 0.5rem 15px;
  margin: 5px;
  color: white;
  display: inline-block;
}}
.kts .code:hover {{
  background-color: {fourth};
}}
.kts .code pre {{
  background-color: {second};
}}
.kts .code:hover pre {{
  background-color: {fourth};
}}
.kts .output {{
  background-color: {second};
  text-align: left;
  border-radius: 15px;
  padding: 5px 15px;
  margin: 5px;
  font-weight: bold;
  font-family: monospace;
  color: {accent};
  overflow: auto;
  max-height: 3rem;
  display: flex;
  flex-direction: column-reverse;
}}

.kts .df {{
  background-color: {second};
  text-align: left;
  border-radius: 15px;
  padding: 5px 15px;
  margin: 5px;
  display: inline-block;
  color: {accent};
}}

.kts .title-with-cross {{
  display: grid;
  grid-template-columns: 1rem auto 1rem;
  margin-left: 5px;
  margin-right: 5px;
}}
.kts .cross-circle {{
  background-color: {second};
  width: 1rem;
  height: 1rem;
  position: relative;
  border-radius: 50%;
  cursor: pointer;
  z-index: 2;
  margin-top: 2px;
}}
.kts .cross-before,
.kts .cross-after {{
  background-color: {third};
  content: '';
  position: absolute;
  width: 0.75rem;
  height: 2px;
  border-radius: 0;
  top: calc((1rem - 2px) / 2);
  z-index: 0;
}}
.kts .cross-before {{
  -webkit-transform: rotate(-45deg);
  -moz-transform: rotate(-45deg);
  transform: rotate(-45deg);
  left: calc(1rem / 8);
}}
.kts .cross-after {{
  -webkit-transform: rotate(-135deg);
  -moz-transform: rotate(-135deg);
  transform: rotate(-135deg);
  right: calc(1rem / 8);
}}

.kts #hidden {{
  display: none
}}
.kts .thumbnail {{
  margin: 0;
  cursor: pointer;
}}
.kts .thumbnail-first {{
  background-color: {first};
}}
.kts .thumbnail-second {{
  background-color: {second};
}}
.kts #collapsible {{
  -webkit-transition: max-height {anim_height}, padding {anim_padding}; 
  -moz-transition: max-height {anim_height}, padding {anim_padding}; 
  -ms-transition: max-height {anim_height}, padding {anim_padding}; 
  -o-transition: max-height {anim_height}, padding {anim_padding}; 
  transition: max-height {anim_height}, padding {anim_padding};  
  
  padding: 0;
  margin: 2px;
  align-self: flex-start;
  max-height: 100px;
  overflow: hidden;
}}
.kts .check {{
  display: none;
}}
.kts .check:checked + #collapsible {{
  padding: 10px;
  max-height: {max_height_expanded};
}}
.kts .check:checked + #collapsible > #hidden {{
  display: inline-flex;
}}
.kts .check:checked + #collapsible > .thumbnail {{
  display: none;
}}
.kts .check:checked + .wrapper-border {{
  border: 2px solid {second};
}}
.kts .check-outer {{
  display: none;
}}
.kts .check-outer:checked + #collapsible {{
  padding: 10px;
  max-height: {max_height_expanded};
}}
.kts .check-outer:checked + #collapsible > #hidden {{
  display: inline-flex;
}}
.kts .check-outer:checked + #collapsible > .thumbnail {{
  display: none;
}}
.kts .check-outer:checked + .wrapper-border {{
  border: 2px solid {second};
}}
.kts .inner-wrapper {{
  flex-direction: column;
}}

.kts progress[value], .kts progress:not([value]) {{
  -webkit-appearance: none;
  appearance: none;
  
  padding: 3px;
  width: calc(100% - 10px);
  height: 1rem;
  box-sizing: content-box;
}}

.kts progress[value]::-webkit-progress-bar {{
  background-color: {second};
  border-radius: 15px;
  padding: 3px;
  box-sizing: border-box;
}}

.kts progress[value]::-webkit-progress-value {{
  background-color: {third};
  border-radius: 15px; 
  box-sizing: border-box;
}}

.kts .hbar-container {{
  display: block;
  position: relative;
  height: min(calc(100% - 3px), 1.5rem);
  margin: 2px;
}}
.kts .hbar {{
  position: absolute;
  display: inline-block;
  background-color: {third};
  text-align: left;
  height: 100%;
  border-radius: 15px;
}}
.kts .hbar-line {{
  position: absolute;
  display: inline-block;
  background-color: {accent};
  text-align: left;
  height: 1px;
  top: 50%;
}}

.kts .inner-column {{
  display: flex;
  flex-direction: column;
  padding: auto;
}}
.kts .row {{
  display: flex;
  flex-direction: row;
}}
"""
