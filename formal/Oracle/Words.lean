/-! Every sequence of events up to a length, one letter an event. -/
namespace Morca.Oracle

/-- Every word one letter longer, by word and then in the letters' order. -/
def extend (letters : List String) : List String → List String
  | [] => []
  | w :: ws => letters.map (fun a => w ++ a) ++ extend letters ws

/-- The words of exactly `n` letters. -/
def words (letters : List String) : Nat → List String
  | 0 => [""]
  | n + 1 => extend letters (words letters n)

/-- The words of one to `n` letters, shortest first. -/
def upTo (letters : List String) : Nat → List String
  | 0 => []
  | n + 1 => upTo letters n ++ words letters (n + 1)

end Morca.Oracle
