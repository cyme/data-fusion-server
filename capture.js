module.exports = Capture;

// Capture
// {
//      previous:       <Capture>,
//      next:           <Capture>,
//      sequence:       #{sequence}
//      value:          "{value}" | #{value} | <Reference>,
// }

function Capture(value, sequence)
{
    this.previous = null;
    this.next = null;
    this.sequence = sequence;
    this.value = value;
}