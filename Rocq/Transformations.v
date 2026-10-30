(* ============================================================================
   Transformations.v

   Coq formalization of:
     "Sound Optimization Transformations for Distributed Systems"

   Every buffer is an operator's input buffer, labeled with a location type
   (where it lives) and an order (whether the operator must consume it
   oldest-first).

   Main results (all Qed; no Admitted):
     - Progress:                wt_out e c → done e ∨ prog_step (e, c) _.
     - Type_preservation:       prog_step preserves wt_out (typing).
     - Trans_Type_preservation: transform B preserves wt_out, for base
                                rewrites B that keep their part typed.
     - soundness:               transform0 preserves reaches_out (both directions).
     - fin_soundness:           transform0 preserves fin_output (both directions).
     - ext_fin_soundness:       transform (rw_ext B), with extra base rewrites B
                                (e.g. partitioning) that satisfy the forward and
                                reverse simulation conditions, preserves
                                fin_output up to chan_equiv (both directions).
   transform B applies one base rewrite from B anywhere in a program; the
   congruence argument for it is proved once, generically (§16).  transform0
   uses this file's base rewrites (relabel), which are an instance (§17).
   ========================================================================== *)

From Stdlib Require Import List.
Import ListNotations.

(* ============================================================================
   1. Parameters: locations, values, operators.
   ========================================================================== *)

Parameter Loc : Type.
Parameter Loc_eq_dec : forall l1 l2 : Loc, {l1 = l2} + {l1 <> l2}.
Parameter Val : Type.
Parameter Op  : Type.

(* ============================================================================
   2. Syntax.
   ========================================================================== *)

(* Channel types: η (network) or l (a location). *)
Inductive CType : Type :=
  | TLoc : Loc -> CType
  | TNet : CType.

(* Order labels: an Ordered buffer is read oldest-first; an Unordered buffer
   may be read in any order. *)
Inductive Order : Type :=
  | Ordered   : Order
  | Unordered : Order.

(* A buffer is just a list of values, newest first. *)
Definition Buffer := list Val.

(* Channels. *)
Inductive Chan : Type :=
  | CEps  : Chan
  | CPair : Chan -> Chan -> Chan
  | CBuf  : Buffer -> CType -> Order -> Chan.

(* Buffer tree β: a nested-pair tree of plain buffers (without labels). *)
Inductive BufTree : Type :=
  | BLeaf : Buffer  -> BufTree
  | BPair : BufTree -> BufTree -> BufTree.

(* β^t : annotate every leaf in β with type t to yield an emission.  An
   emission's order labels are placeholders: ⧺ keeps the order label of the
   buffer it is appended to. *)
Fixpoint wrap_loc (β : BufTree) (t : CType) : Chan :=
  match β with
  | BLeaf b      => CBuf b t Unordered
  | BPair β1 β2  => CPair (wrap_loc β1 t) (wrap_loc β2 t)
  end.

(* Path directions (L / R). *)
Inductive Dir : Type := DL | DR.
Definition Path := list Dir.

(* Operator semantics: given a path π and a value v, an operator yields the
   next operator and a (typeless) buffer tree:  o(π, v) = 〈o', β〉.  β is
   emitted at once, stamped with the operator's location: β^l. *)
Parameter apply_op : Op -> Path -> Val -> Op * BufTree.

(* Expressions (dataflows).  EOp c o l : operator o at location l with input
   channel c.  A whole program pairs an expression with its accumulated
   output channel (§7). *)
Inductive Expr : Type :=
  | EPar : Expr -> Expr -> Expr
  | ESeq : Expr -> Expr -> Expr
  | EOp  : Chan -> Op -> Loc -> Expr.

(* ============================================================================
   3. Path access for channels and expressions.
   ========================================================================== *)

Fixpoint chan_get (c : Chan) (π : Path) {struct π} : option Chan :=
  match π with
  | [] => Some c
  | DL :: π' =>
      match c with
      | CPair c1 _ => chan_get c1 π'
      | _ => None
      end
  | DR :: π' =>
      match c with
      | CPair _ c2 => chan_get c2 π'
      | _ => None
      end
  end.

Fixpoint chan_set (c : Chan) (π : Path) (c' : Chan) {struct π} : option Chan :=
  match π with
  | [] => Some c'
  | DL :: π' =>
      match c with
      | CPair c1 c2 =>
          match chan_set c1 π' c' with
          | Some c1' => Some (CPair c1' c2)
          | None => None
          end
      | _ => None
      end
  | DR :: π' =>
      match c with
      | CPair c1 c2 =>
          match chan_set c2 π' c' with
          | Some c2' => Some (CPair c1 c2')
          | None => None
          end
      | _ => None
      end
  end.

(* b^(t,o) ∈ c  ≔  ∃π. c[π] = b^(t,o) *)
Definition buf_in (b : Buffer) (t : CType) (o : Order) (c : Chan) : Prop :=
  exists π, chan_get c π = Some (CBuf b t o).

Fixpoint expr_get (e : Expr) (π : Path) {struct π} : option Expr :=
  match π with
  | [] => Some e
  | DL :: π' =>
      match e with
      | EPar e1 _ => expr_get e1 π'
      | _ => None
      end
  | DR :: π' =>
      match e with
      | EPar _ e2 => expr_get e2 π'
      | _ => None
      end
  end.

(* ============================================================================
   4. Channel concatenation ⧺.

      Leaf combination is asymmetric — TNet absorbs a same-located TLoc on
      the LEFT, but rejects it on the right:
        b₁ˡ  ⧺ b₂ˡ  ≔ (b₁ ⧺ b₂)ˡ          (locations must match)
        b₁ˡ  ⧺ b₂^η ≔ (b₁ ⧺ b₂)^η
        b₁^η ⧺ b₂^η ≔ (b₁ ⧺ b₂)^η
        b₁^η ⧺ b₂ˡ  is undefined
      The result keeps the right buffer's order label.  Pair-wise on CPair,
      identity ε ⧺ c = c, and c ⧺ ε is undefined for non-ε c.
   ========================================================================== *)

Definition ctype_combine (t1 t2 : CType) : option CType :=
  match t1, t2 with
  | TLoc l1, TLoc l2 =>
      if Loc_eq_dec l1 l2 then Some (TLoc l1) else None
  | TLoc _, TNet  => Some TNet
  | TNet,   TNet  => Some TNet
  | TNet,   TLoc _ => None
  end.

Fixpoint chan_concat (c1 c2 : Chan) : option Chan :=
  match c1 with
  | CEps => Some c2
  | CBuf b1 t1 _ =>
      match c2 with
      | CBuf b2 t2 o2 =>
          match ctype_combine t1 t2 with
          | Some t => Some (CBuf (b1 ++ b2) t o2)
          | None => None
          end
      | _ => None
      end
  | CPair c1a c1b =>
      match c2 with
      | CPair c2a c2b =>
          match chan_concat c1a c2a with
          | Some ra =>
              match chan_concat c1b c2b with
              | Some rb => Some (CPair ra rb)
              | None => None
              end
          | None => None
          end
      | _ => None
      end
  end.

(* ============================================================================
   5. Feed input  (c ⊳⁺ e).
   ========================================================================== *)

Fixpoint feed (c : Chan) (e : Expr) : option Expr :=
  match e with
  | EPar e1 e2 =>
      match c with
      | CEps => Some (EPar e1 e2)   (* ε ⊳⁺ (e | e') ≔ (e | e') *)
      | CPair c1 c2 =>
          match feed c1 e1 with
          | Some e1' =>
              match feed c2 e2 with
              | Some e2' => Some (EPar e1' e2')
              | None => None
              end
          | None => None
          end
      | _ => None
      end
  | ESeq e1 e2 =>
      match feed c e1 with
      | Some e1' => Some (ESeq e1' e2)
      | None     => None
      end
  | EOp c0 o l =>
      match chan_concat c c0 with
      | Some c' => Some (EOp c' o l)
      | None    => None
      end
  end.

(* ============================================================================
   6. Input channel projection  (e |_⊳).
   ========================================================================== *)

Fixpoint inp_chan (e : Expr) : option Chan :=
  match e with
  | EPar e1 e2 =>
      match inp_chan e1 with
      | Some c1 =>
          match inp_chan e2 with
          | Some c2 => Some (CPair c1 c2)
          | None => None
          end
      | None => None
      end
  | ESeq e1 _ => inp_chan e1
  | EOp c _ _ => Some c
  end.

(* ============================================================================
   6b. Operator ordering modes.

      Each operator declares whether it needs ordered input (NeedOrdered) or
      accepts any (AnyOrder), and whether its output is ordered: never
      (OutUnordered), always (OutOrdered), or exactly when all its input
      buffers are (OutPreserve).  Modes only constrain which labels a
      well-typed program may give the buffers around the operator (§8); at
      runtime, reading is governed by the labels themselves.
   ========================================================================== *)

Inductive InReq : Type :=
  | NeedOrdered : InReq
  | AnyOrder    : InReq.

Inductive OutMode : Type :=
  | OutUnordered : OutMode
  | OutPreserve  : OutMode
  | OutOrdered   : OutMode.

Parameter Op_mode : Op -> InReq * OutMode.

(* Stepping an operator does not change its ordering modes. *)
Axiom apply_op_mode_preserved :
  forall o π v o' β,
    apply_op o π v = (o', β) ->
    Op_mode o' = Op_mode o.

(* o needs ordered input. *)
Definition reads_fifo (o : Op) : Prop := fst (Op_mode o) = NeedOrdered.

(* ============================================================================
   7. Operational semantics.

      step e e' c   represents  e → e' ► c : the expression e takes one step
                                            to e', emitting the channel c.
      A program is an expression together with its accumulated output; a
      program step (prog_step) appends the emitted channel to that output.

      An operator step (S_Op) consumes one input value — the oldest of its
      buffer if that buffer is Ordered, any otherwise — and emits its result
      stamped with the operator's location.
   ========================================================================== *)

Inductive step : Expr -> Expr -> Chan -> Prop :=
  | S_Par_L :
      forall e1 e2 e1' c1',
        step e1 e1' c1' ->
        step (EPar e1 e2) (EPar e1' e2) (CPair c1' CEps)

  | S_Par_R :
      forall e1 e2 e2' c2',
        step e2 e2' c2' ->
        step (EPar e1 e2) (EPar e1 e2') (CPair CEps c2')

  | S_Seq_L :
      forall e1 e2 e1' c e2',
        step e1 e1' c ->
        feed c e2 = Some e2' ->
        step (ESeq e1 e2) (ESeq e1' e2') CEps

  | S_Seq_R :
      forall e1 e2 e2' c,
        step e2 e2' c ->
        step (ESeq e1 e2) (ESeq e1 e2') c

  | S_Op :
      forall c o l π v t ord b1 b2 c' o' β,
        chan_get c π = Some (CBuf (b1 ++ v :: b2) t ord) ->
        (ord = Ordered -> b2 = []) ->
        (t = TLoc l \/ t = TNet) ->
        chan_set c π (CBuf (b1 ++ b2) t ord) = Some c' ->
        apply_op o π v = (o', β) ->
        step (EOp c o l) (EOp c' o' l) (wrap_loc β (TLoc l)).

(* A program step: the expression steps, and its emission is appended to the
   program's accumulated output (Doc.tex rule W-Out:  e ► c → e' ► (c' ⧺ c)). *)
Inductive prog_step : Expr * Chan -> Expr * Chan -> Prop :=
  | P_Step :
      forall e e' c c' c'',
        step e e' c' ->
        chan_concat c' c = Some c'' ->
        prog_step (e, c) (e', c'').

(* Reflexive-transitive closure of prog_step. *)
Inductive prog_star : Expr * Chan -> Expr * Chan -> Prop :=
  | PS_refl : forall x, prog_star x x
  | PS_step : forall x y z,
      prog_step x y -> prog_star y z -> prog_star x z.

(* ============================================================================
   8. Type system (per Doc.tex §"Type system").

      Type trees τ have leaves labeled with a location type and an order.  A
      buffer of type (t, o) can carry a stream of type (t', o') when t <: t'
      (Sub-Refl, Sub-NL: TNet <: TLoc l) and o = Ordered → o' = Ordered.
      Doc.tex declares ε|_T undefined, so user-level channels (including the
      outer `c` in Γ ⊢ e ► c) must be non-CEps; CEps only appears transiently
      in step outputs (e.g., CPair c CEps from S_Par_L).  Two predicates:
        - chan_sub_type c τ — strict (non-CEps), with Sub-NL slack; used for
                              input ascription and outer-context typing.
        - chan_at_loc c τ   — exact location match (no Sub-NL slack), and
                              allows CEps anywhere; the form step outputs
                              satisfy.

      An operator's output is ordered according to its OutMode and the order
      labels of its input buffers (out_ord); an operator that reads FIFO needs
      all its input buffers Ordered.  Runtime axioms on apply_op carry the
      operator-signature invariants (apply_op_shape, apply_op_sig_preserved,
      apply_op_mode_preserved).
   ========================================================================== *)

Inductive type_tree : Type :=
  | TTLeaf : CType -> Order -> type_tree
  | TTPair : type_tree -> type_tree -> type_tree.

Inductive shape_tree : Type :=
  | STLeaf : shape_tree
  | STPair : shape_tree -> shape_tree -> shape_tree.

(* Sub-Refl, Sub-NL on location types. *)
Inductive sub_leaf : CType -> CType -> Prop :=
  | SL_Refl : forall t, sub_leaf t t
  | SL_NL   : forall l, sub_leaf TNet (TLoc l).

(* A buffer labeled o can carry a stream ordered o'. *)
Definition ord_le (o o' : Order) : Prop := o = Ordered -> o' = Ordered.

(* c |_T <: τ — strict structural subtyping, no CEps (since ε|_T is undefined). *)
Inductive chan_sub_type : Chan -> type_tree -> Prop :=
  | CST_CBuf  : forall b t t' o o',
      sub_leaf t t' -> ord_le o o' -> chan_sub_type (CBuf b t o) (TTLeaf t' o')
  | CST_CPair : forall c1 c2 a b,
      chan_sub_type c1 a -> chan_sub_type c2 b ->
      chan_sub_type (CPair c1 c2) (TTPair a b).

(* Step-output channel form: leaves exactly at τ's locations (no Sub-NL
   relaxation); CEps anywhere. *)
Inductive chan_at_loc : Chan -> type_tree -> Prop :=
  | CAL_CEps  : forall τ, chan_at_loc CEps τ
  | CAL_CBuf  : forall b t o o', chan_at_loc (CBuf b t o) (TTLeaf t o')
  | CAL_CPair : forall c1 c2 a b,
      chan_at_loc c1 a -> chan_at_loc c2 b ->
      chan_at_loc (CPair c1 c2) (TTPair a b).

(* κ^(t,o) — label every leaf of κ with type t and order o. *)
Fixpoint shape_to_type (κ : shape_tree) (t : CType) (o : Order) : type_tree :=
  match κ with
  | STLeaf       => TTLeaf t o
  | STPair k1 k2 => TTPair (shape_to_type k1 t o) (shape_to_type k2 t o)
  end.

(* β has shape κ — pair-tree skeleton matches. *)
Inductive bt_shape : BufTree -> shape_tree -> Prop :=
  | BS_Leaf : forall b, bt_shape (BLeaf b) STLeaf
  | BS_Pair : forall b1 b2 k1 k2,
      bt_shape b1 k1 -> bt_shape b2 k2 ->
      bt_shape (BPair b1 b2) (STPair k1 k2).

(* Operator signatures (parameter Γ). *)
Parameter Op_sig : Op -> shape_tree * shape_tree.

(* Runtime well-formedness axiom: apply_op respects the operator signature
   (its output β has shape κ2). *)
Axiom apply_op_shape :
  forall o π v o' β κ1 κ2,
    Op_sig o = (κ1, κ2) ->
    apply_op o π v = (o', β) ->
    bt_shape β κ2.

(* Every buffer of c is Ordered. *)
Fixpoint chan_all_ordered (c : Chan) : bool :=
  match c with
  | CEps                => true
  | CBuf _ _ Ordered    => true
  | CBuf _ _ Unordered  => false
  | CPair c1 c2         => chan_all_ordered c1 && chan_all_ordered c2
  end.

(* The order of o's output when its input buffers are c. *)
Definition out_ord (o : Op) (c : Chan) : Order :=
  match snd (Op_mode o) with
  | OutUnordered => Unordered
  | OutOrdered   => Ordered
  | OutPreserve  => if chan_all_ordered c then Ordered else Unordered
  end.

(* Γ ⊢ e : τ — W-Par, W-Seq, W-Op (Doc.tex §"Type system").  In W_Op_t the
   input buffers must be local to l or network buffers; the order Ordered in
   their ascribed type imposes no constraint (every buffer can carry an
   ordered stream).  Order requirements are reads_fifo's. *)
Inductive wt_inner : Expr -> type_tree -> Prop :=
  | W_Par_t : forall e1 e2 τ1 τ2,
      wt_inner e1 τ1 -> wt_inner e2 τ2 ->
      wt_inner (EPar e1 e2) (TTPair τ1 τ2)
  | W_Seq_t : forall e1 e2 τ1 τ2 c_in,
      wt_inner e1 τ1 ->
      inp_chan e2 = Some c_in ->
      chan_sub_type c_in τ1 ->
      wt_inner e2 τ2 ->
      wt_inner (ESeq e1 e2) τ2
  | W_Op_t : forall c o l κ1 κ2,
      Op_sig o = (κ1, κ2) ->
      chan_sub_type c (shape_to_type κ1 (TLoc l) Ordered) ->
      (reads_fifo o -> chan_all_ordered c = true) ->
      wt_inner (EOp c o l) (shape_to_type κ2 (TLoc l) (out_ord o c)).

(* Γ ⊢ e ► c (W-Out): the program (e, c) is well-typed.  The outer channel
   c is sub-typed against e's type τ (and so non-CEps, since ε|_T is undefined). *)
Definition wt_out (e : Expr) (c : Chan) : Prop :=
  exists τ, wt_inner e τ /\ chan_sub_type c τ.

(* e ↓ — "done" per Doc.tex's F-rules.  An EOp is done when all its input
   buffers are empty; EPar/ESeq are done when both children are. *)
Inductive done : Expr -> Prop :=
  | F_Par : forall e1 e2, done e1 -> done e2 -> done (EPar e1 e2)
  | F_Seq : forall e1 e2, done e1 -> done e2 -> done (ESeq e1 e2)
  | F_Op  : forall c o l,
      (forall b t ord, buf_in b t ord c -> b = []) ->
      done (EOp c o l).

(* ============================================================================
   9. Transformations.

      transform B e e' c : e ► c ↣ e' ► c — the dataflow e in output context c
      can be rewritten to e' with the SAME output context, by one base rewrite
      from B applied at some position.  The congruence rules carry the output
      context of each position down to it, so a base rewrite can constrain its
      own context.

      This file's base rewrites (relabel) only change one label or location:
        - L2N: an input buffer local to its operator becomes a network buffer;
        - NLN: an operator surrounded by network buffers moves;
        - LNL: a network buffer between two operators at l becomes local.
      Other files add base rewrites with rw_ext (e.g. partitioning).
   ========================================================================== *)

Definition all_net (c : Chan) : Prop :=
  forall b t ord, buf_in b t ord c -> t = TNet.

Inductive relabel : Expr -> Expr -> Chan -> Prop :=
  | R_L2N : forall c c' o l π b ord c_outer,
      chan_get c π = Some (CBuf b (TLoc l) ord) ->
      chan_set c π (CBuf b TNet ord) = Some c' ->
      relabel (EOp c o l) (EOp c' o l) c_outer

  | R_NLN : forall c o l l' c_outer,
      all_net c -> all_net c_outer ->
      relabel (EOp c o l) (EOp c o l') c_outer

  | R_LNL : forall e1 π c1 o1 l c c' o2 b ord c_outer,
      expr_get e1 π = Some (EOp c1 o1 l) ->
      chan_get c π = Some (CBuf b TNet ord) ->
      chan_set c π (CBuf b (TLoc l) ord) = Some c' ->
      relabel (ESeq e1 (EOp c o2 l)) (ESeq e1 (EOp c' o2 l)) c_outer.

Inductive transform (B : Expr -> Expr -> Chan -> Prop) : Expr -> Expr -> Chan -> Prop :=
  | T_Par_L : forall e1 e1' e2 c1 c2,
      transform B e1 e1' c1 ->
      transform B (EPar e1 e2) (EPar e1' e2) (CPair c1 c2)

  | T_Par_R : forall e1 e2 e2' c1 c2,
      transform B e2 e2' c2 ->
      transform B (EPar e1 e2) (EPar e1 e2') (CPair c1 c2)

  | T_Seq_L : forall e1 e1' e2 c_outer c_in,
      inp_chan e2 = Some c_in ->
      transform B e1 e1' c_in ->
      transform B (ESeq e1 e2) (ESeq e1' e2) c_outer

  | T_Seq_R : forall e1 e2 e2' c_outer,
      transform B e2 e2' c_outer ->
      transform B (ESeq e1 e2) (ESeq e1 e2') c_outer

  | T_Base : forall e e' c_outer,
      B e e' c_outer ->
      transform B e e' c_outer.

(* This file's transformations, and those together with extra base rewrites B. *)
Abbreviation transform0 := (transform relabel).
Definition rw_ext (B : Expr -> Expr -> Chan -> Prop) : Expr -> Expr -> Chan -> Prop :=
  fun e e' c => relabel e e' c \/ B e e' c.

Lemma transform_mono :
  forall (B B' : Expr -> Expr -> Chan -> Prop),
    (forall e e' c, B e e' c -> B' e e' c) ->
    forall e e' c, transform B e e' c -> transform B' e e' c.
Proof.
  intros B B' HB e e' c H. induction H; [apply T_Par_L | apply T_Par_R | eapply T_Seq_L
                                         | apply T_Seq_R | apply T_Base; auto]; eauto.
Qed.

(* ============================================================================
   10. Channel-set inversion and divergence.
   ========================================================================== *)

(* Helper: setting at a path inverts via getting then setting back. *)
Lemma chan_set_get_inv :
  forall π c v c',
    chan_set c π v = Some c' ->
    chan_get c' π = Some v.
Proof.
  induction π as [|[|] π IH]; intros c v c' Hset.
  - simpl in Hset. inversion Hset; subst. reflexivity.
  - simpl in Hset. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set ca π v) as [ca'|] eqn:E; try discriminate.
    inversion Hset; subst. simpl. eapply IH; eauto.
  - simpl in Hset. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set cb π v) as [cb'|] eqn:E; try discriminate.
    inversion Hset; subst. simpl. eapply IH; eauto.
Qed.

(* Helper: setting back to the original value gives the original. *)
Lemma chan_set_set_back :
  forall π c v_orig c1,
    chan_get c π = Some v_orig ->
    chan_set c π v_orig = Some c1 ->
    c1 = c.
Proof.
  induction π as [|[|] π IH]; intros c v_orig c1 Hget Hset.
  - simpl in *. inversion Hget; subst. inversion Hset; subst. reflexivity.
  - simpl in *. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set ca π v_orig) as [ca'|] eqn:E; try discriminate.
    inversion Hset; subst.
    f_equal.
    eapply IH; eauto.
  - simpl in *. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set cb π v_orig) as [cb'|] eqn:E; try discriminate.
    inversion Hset; subst.
    f_equal.
    eapply IH; eauto.
Qed.

(* The set inverse: if c' = c[π ↦ v], then c = c'[π ↦ v_orig] (assuming c[π] = v_orig). *)
Lemma chan_set_inverse :
  forall π c v_orig v_new c',
    chan_get c π = Some v_orig ->
    chan_set c π v_new = Some c' ->
    chan_set c' π v_orig = Some c.
Proof.
  induction π as [|[|] π IH]; intros c v_orig v_new c' Hget Hset.
  - simpl in *. inversion Hget; subst. inversion Hset; subst. reflexivity.
  - simpl in *. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set ca π v_new) as [ca'|] eqn:E; try discriminate.
    inversion Hset; subst.
    simpl.
    erewrite IH; eauto.
  - simpl in *. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set cb π v_new) as [cb'|] eqn:E; try discriminate.
    inversion Hset; subst.
    simpl.
    erewrite IH; eauto.
Qed.

(* Path equality is decidable. *)
Definition dir_eq_dec : forall (d1 d2 : Dir), {d1 = d2} + {d1 <> d2}.
Proof. decide equality. Defined.

Definition path_eq_dec : forall (π1 π2 : Path), {π1 = π2} + {π1 <> π2}.
Proof. apply List.list_eq_dec. exact dir_eq_dec. Defined.

(* The key helper: setting a buffer at π1 and asking for a buffer at π2 — the
   only possibilities are (π1 = π2, get returns the new value) or (π2 reaches
   a buffer untouched by the set).  The prefix cases are ruled out because
   navigating into a CBuf at a non-empty extra path yields None. *)
Lemma chan_set_buf_get_buf :
  forall π1 π2 c b1 t1 o1 v c' b t o,
    chan_get c π1 = Some (CBuf b1 t1 o1) ->
    chan_set c π1 v = Some c' ->
    chan_get c π2 = Some (CBuf b t o) ->
    (π1 = π2 /\ chan_get c' π2 = Some v) \/
    (π1 <> π2 /\ chan_get c' π2 = Some (CBuf b t o)).
Proof.
  induction π1 as [|d1 π1' IH]; intros π2 c b1 t1 o1 v c' b t o Hget1 Hset Hget2.
  - (* π1 = [] *)
    simpl in Hget1, Hset. inversion Hget1; subst. inversion Hset; subst.
    destruct π2 as [|d2 π2'].
    + left. split; reflexivity.
    + simpl in Hget2. destruct d2; discriminate.
  - (* π1 = d1 :: π1' *)
    destruct c as [|cL cR|bb tt oo].
    + (* CEps *) simpl in Hset. destruct d1; discriminate.
    + (* CPair *)
      destruct d1.
      * (* DL *)
        simpl in Hset. simpl in Hget1.
        destruct (chan_set cL π1' v) as [cL'|] eqn:EsL; try discriminate.
        inversion Hset; subst.
        destruct π2 as [|d2 π2'].
        -- simpl in Hget2. discriminate.
        -- destruct d2.
           ++ simpl in Hget2.
              specialize (IH π2' cL b1 t1 o1 v cL' b t o Hget1 EsL Hget2).
              simpl.
              destruct IH as [[Heq Hgc']|[Hne Hgc']].
              ** subst π2'. left. split; [reflexivity | exact Hgc'].
              ** right. split.
                 +++ intros Heq. apply Hne. inversion Heq. reflexivity.
                 +++ exact Hgc'.
           ++ simpl in Hget2.
              right. split.
              ** intros Heq. inversion Heq.
              ** simpl. exact Hget2.
      * (* DR *)
        simpl in Hset. simpl in Hget1.
        destruct (chan_set cR π1' v) as [cR'|] eqn:EsR; try discriminate.
        inversion Hset; subst.
        destruct π2 as [|d2 π2'].
        -- simpl in Hget2. discriminate.
        -- destruct d2.
           ++ simpl in Hget2.
              right. split.
              ** intros Heq. inversion Heq.
              ** simpl. exact Hget2.
           ++ simpl in Hget2.
              specialize (IH π2' cR b1 t1 o1 v cR' b t o Hget1 EsR Hget2).
              simpl.
              destruct IH as [[Heq Hgc']|[Hne Hgc']].
              ** subst π2'. left. split; [reflexivity | exact Hgc'].
              ** right. split.
                 +++ intros Heq. apply Hne. inversion Heq. reflexivity.
                 +++ exact Hgc'.
    + (* CBuf *) simpl in Hset. destruct d1; discriminate.
Qed.

(* Helper: setting twice at the same path — the second set wins. *)
Lemma chan_set_set_same :
  forall π c v1 c1 v2 c2,
    chan_set c π v1 = Some c1 ->
    chan_set c1 π v2 = Some c2 ->
    chan_set c π v2 = Some c2.
Proof.
  induction π as [|d π' IH]; intros c v1 c1 v2 c2 Hset1 Hset2.
  - simpl in *. inversion Hset1; subst. inversion Hset2; subst. reflexivity.
  - destruct c as [|cL cR|bb tt oo].
    + (* CEps *) simpl in Hset1. destruct d; discriminate.
    + (* CPair *) destruct d.
      * simpl in Hset1.
        destruct (chan_set cL π' v1) as [cL1|] eqn:EsL1; try discriminate.
        inversion Hset1; subst.
        simpl in Hset2.
        destruct (chan_set cL1 π' v2) as [cL2|] eqn:EsL2; try discriminate.
        inversion Hset2; subst.
        simpl. erewrite IH; eauto.
      * simpl in Hset1.
        destruct (chan_set cR π' v1) as [cR1|] eqn:EsR1; try discriminate.
        inversion Hset1; subst.
        simpl in Hset2.
        destruct (chan_set cR1 π' v2) as [cR2|] eqn:EsR2; try discriminate.
        inversion Hset2; subst.
        simpl. erewrite IH; eauto.
    + (* CBuf *) simpl in Hset1. destruct d; discriminate.
Qed.

(* Helper: two sets at the same path — going via c1 gives same final result as
   going directly. *)
Lemma chan_set_replace :
  forall π c v1 v2 c1 c2,
    chan_set c π v1 = Some c1 ->
    chan_set c π v2 = Some c2 ->
    chan_set c1 π v2 = Some c2.
Proof.
  induction π as [|d π' IH]; intros c v1 v2 c1 c2 H1 H2.
  - simpl in *. inversion H1; subst. inversion H2; subst. reflexivity.
  - destruct c as [|cL cR|bb tt oo].
    + simpl in H1. destruct d; discriminate.
    + destruct d.
      * simpl in H1, H2.
        destruct (chan_set cL π' v1) as [cL1|] eqn:EL1; try discriminate.
        destruct (chan_set cL π' v2) as [cL2|] eqn:EL2; try discriminate.
        inversion H1; subst. inversion H2; subst.
        simpl. erewrite IH; eauto.
      * simpl in H1, H2.
        destruct (chan_set cR π' v1) as [cR1|] eqn:ER1; try discriminate.
        destruct (chan_set cR π' v2) as [cR2|] eqn:ER2; try discriminate.
        inversion H1; subst. inversion H2; subst.
        simpl. erewrite IH; eauto.
    + simpl in H1. destruct d; discriminate.
Qed.

(* Helper: chan_set succeeds at any path where there's a CBuf. *)
Lemma chan_set_at_buf_exists :
  forall π c b t o v,
    chan_get c π = Some (CBuf b t o) ->
    exists c', chan_set c π v = Some c'.
Proof.
  induction π as [|d π' IH]; intros c b t o v Hget.
  - simpl in *. inversion Hget; subst. exists v. reflexivity.
  - destruct c as [|cL cR|bb tt oo].
    + simpl in Hget. destruct d; discriminate.
    + destruct d.
      * simpl in Hget. destruct (IH cL b t o v Hget) as [cL' HL].
        simpl. rewrite HL. eexists. reflexivity.
      * simpl in Hget. destruct (IH cR b t o v Hget) as [cR' HR].
        simpl. rewrite HR. eexists. reflexivity.
    + simpl in Hget. destruct d; discriminate.
Qed.

(* Helper: chan_set commutes at diverging paths (both reach buffers, paths differ). *)
Lemma chan_set_diverge_commute :
  forall π1 π2 c b1 t1 o1 b2 t2 o2 v1 v2 c1 c2 c12,
    chan_get c π1 = Some (CBuf b1 t1 o1) ->
    chan_get c π2 = Some (CBuf b2 t2 o2) ->
    π1 <> π2 ->
    chan_set c π1 v1 = Some c1 ->
    chan_set c π2 v2 = Some c2 ->
    chan_set c1 π2 v2 = Some c12 ->
    chan_set c2 π1 v1 = Some c12.
Proof.
  induction π1 as [|d1 π1' IH]; intros π2 c b1 t1 o1 b2 t2 o2 v1 v2 c1 c2 c12
                                       Hg1 Hg2 Hne Hs1 Hs2 Hs12.
  - (* π1 = [] *)
    simpl in Hg1, Hs1. inversion Hg1; subst. inversion Hs1; subst.
    (* So c = CBuf b1 t1, c1 = v1.  But chan_get (CBuf b1 t1) π2 = Some (CBuf b2 t2)
       requires π2 = [].  Then π1 = π2 = [], contradicting Hne. *)
    destruct π2 as [|d2 π2'].
    + exfalso. apply Hne. reflexivity.
    + simpl in Hg2. destruct d2; discriminate.
  - (* π1 = d1 :: π1' *)
    destruct c as [|cL cR|? ? ?]; try (simpl in Hg1; destruct d1; discriminate).
    destruct d1.
    + (* π1 starts with DL *)
      simpl in Hg1. simpl in Hs1.
      destruct (chan_set cL π1' v1) as [cL1|] eqn:EL1; try discriminate.
      inversion Hs1; subst c1.
      destruct π2 as [|d2 π2'].
      * simpl in Hg2. discriminate.
      * destruct d2.
        -- (* DL :: π2', both left *)
           simpl in Hg2. simpl in Hs2.
           destruct (chan_set cL π2' v2) as [cL2|] eqn:EL2; try discriminate.
           inversion Hs2; subst c2.
           simpl in Hs12.
           destruct (chan_set cL1 π2' v2) as [cL12|] eqn:EL12; try discriminate.
           inversion Hs12; subst c12.
           assert (Hne' : π1' <> π2').
           { intros Heq. apply Hne. f_equal. exact Heq. }
           assert (Hcom : chan_set cL2 π1' v1 = Some cL12).
           { eapply IH; eauto. }
           simpl. rewrite Hcom. reflexivity.
        -- (* DL :: π1', DR :: π2': different branches *)
           simpl in Hg2. simpl in Hs2.
           destruct (chan_set cR π2' v2) as [cR2|] eqn:ER2; try discriminate.
           inversion Hs2; subst c2.
           simpl in Hs12.
           rewrite ER2 in Hs12. inversion Hs12; subst c12.
           simpl. rewrite EL1. reflexivity.
    + (* π1 starts with DR *)
      simpl in Hg1. simpl in Hs1.
      destruct (chan_set cR π1' v1) as [cR1|] eqn:ER1; try discriminate.
      inversion Hs1; subst c1.
      destruct π2 as [|d2 π2'].
      * simpl in Hg2. discriminate.
      * destruct d2.
        -- (* DR :: π1', DL :: π2': different branches *)
           simpl in Hg2. simpl in Hs2.
           destruct (chan_set cL π2' v2) as [cL2|] eqn:EL2; try discriminate.
           inversion Hs2; subst c2.
           simpl in Hs12.
           rewrite EL2 in Hs12. inversion Hs12; subst c12.
           simpl. rewrite ER1. reflexivity.
        -- (* DR :: π2', both right *)
           simpl in Hg2. simpl in Hs2.
           destruct (chan_set cR π2' v2) as [cR2|] eqn:ER2; try discriminate.
           inversion Hs2; subst c2.
           simpl in Hs12.
           destruct (chan_set cR1 π2' v2) as [cR12|] eqn:ER12; try discriminate.
           inversion Hs12; subst c12.
           assert (Hne' : π1' <> π2').
           { intros Heq. apply Hne. f_equal. exact Heq. }
           assert (Hcom : chan_set cR2 π1' v1 = Some cR12).
           { eapply IH; eauto. }
           simpl. rewrite Hcom. reflexivity.
Qed.

(* ============================================================================
   11. Labels under chan_set and ⧺, and all_net.
   ========================================================================== *)

Lemma buf_in_cbuf : forall b t o b' t' o',
  buf_in b t o (CBuf b' t' o') -> b = b' /\ t = t' /\ o = o'.
Proof.
  intros b t o b' t' o' [[|[|] π] Hg]; simpl in Hg; try discriminate.
  inversion Hg; subst. auto.
Qed.

Lemma buf_in_pair : forall b t o c1 c2,
  buf_in b t o (CPair c1 c2) -> buf_in b t o c1 \/ buf_in b t o c2.
Proof.
  intros b t o c1 c2 [[|[|] π] Hg]; simpl in Hg; try discriminate.
  - left. exists π. exact Hg.
  - right. exists π. exact Hg.
Qed.

(* A buffer after a chan_set is one of the old buffers or one of the new value's. *)
Lemma chan_set_buf_in : forall π c v c' b t o,
  chan_set c π v = Some c' -> buf_in b t o c' ->
  buf_in b t o c \/ buf_in b t o v.
Proof.
  induction π as [|d π IH]; intros c v c' b t o Hset [π0 Hg].
  - simpl in Hset. inversion Hset; subst. right. exists π0. exact Hg.
  - destruct d, c as [|ca cb|? ? ?]; simpl in Hset; try discriminate.
    + destruct (chan_set ca π v) as [ca'|] eqn:E; try discriminate. inversion Hset; subst.
      destruct π0 as [|[|] π0]; simpl in Hg; try discriminate.
      * destruct (IH _ _ _ b t o E (ex_intro _ π0 Hg)) as [[π1 H1]|H1].
        -- left. exists (DL :: π1). exact H1.
        -- right. exact H1.
      * left. exists (DR :: π0). exact Hg.
    + destruct (chan_set cb π v) as [cb'|] eqn:E; try discriminate. inversion Hset; subst.
      destruct π0 as [|[|] π0]; simpl in Hg; try discriminate.
      * left. exists (DL :: π0). exact Hg.
      * destruct (IH _ _ _ b t o E (ex_intro _ π0 Hg)) as [[π1 H1]|H1].
        -- left. exists (DR :: π1). exact H1.
        -- right. exact H1.
Qed.

(* ⧺ keeps the right side's labels. *)
Lemma ctype_combine_r : forall t1 t2 t, ctype_combine t1 t2 = Some t -> t = t2.
Proof.
  intros [l1|] [l2|] t H; simpl in H; try discriminate.
  - destruct (Loc_eq_dec l1 l2); [inversion H; subst; reflexivity | discriminate].
  - inversion H; reflexivity.
  - inversion H; reflexivity.
Qed.

Lemma chan_concat_buf_in : forall c1 c2 r b t o,
  chan_concat c1 c2 = Some r -> buf_in b t o r -> exists b', buf_in b' t o c2.
Proof.
  induction c1 as [|c1a IHa c1b IHb|b1 t1 o1]; intros c2 r b t o Hcc Hin.
  - simpl in Hcc. inversion Hcc; subst. exists b. exact Hin.
  - destruct c2 as [|c2a c2b|? ? ?]; simpl in Hcc; try discriminate.
    destruct (chan_concat c1a c2a) as [ra|] eqn:Ea; try discriminate.
    destruct (chan_concat c1b c2b) as [rb|] eqn:Eb; try discriminate.
    inversion Hcc; subst.
    destruct (buf_in_pair _ _ _ _ _ Hin) as [H|H].
    + destruct (IHa _ _ _ _ _ Ea H) as [b' [π Hg]]. exists b', (DL :: π). exact Hg.
    + destruct (IHb _ _ _ _ _ Eb H) as [b' [π Hg]]. exists b', (DR :: π). exact Hg.
  - destruct c2 as [|? ?|b2 t2 o2]; simpl in Hcc; try discriminate.
    destruct (ctype_combine t1 t2) as [t'|] eqn:Ect; try discriminate.
    inversion Hcc; subst.
    destruct (buf_in_cbuf _ _ _ _ _ _ Hin) as (-> & -> & ->).
    rewrite (ctype_combine_r _ _ _ Ect).
    exists b2, []. reflexivity.
Qed.

Lemma all_net_chan_get_TNet :
  forall c π b t o,
    all_net c -> chan_get c π = Some (CBuf b t o) -> t = TNet.
Proof.
  intros c π b t o Han Hget. apply (Han b t o). exists π. exact Hget.
Qed.

Lemma all_net_pair_l : forall c1 c2, all_net (CPair c1 c2) -> all_net c1.
Proof. intros c1 c2 Han b t o [π Hget]. apply (Han b t o). exists (DL :: π). exact Hget. Qed.

Lemma all_net_pair_r : forall c1 c2, all_net (CPair c1 c2) -> all_net c2.
Proof. intros c1 c2 Han b t o [π Hget]. apply (Han b t o). exists (DR :: π). exact Hget. Qed.

Lemma chan_set_TNet_preserves_all_net :
  forall c π b o cnew,
    all_net c ->
    chan_set c π (CBuf b TNet o) = Some cnew ->
    all_net cnew.
Proof.
  intros c π b o cnew Han Hset b0 t o0 Hin.
  destruct (chan_set_buf_in _ _ _ _ _ _ _ Hset Hin) as [H|H].
  - exact (Han _ _ _ H).
  - destruct (buf_in_cbuf _ _ _ _ _ _ H) as (_ & -> & _). reflexivity.
Qed.

(* ⧺ onto an all-net channel yields an all-net channel. *)
Lemma chan_concat_all_net_r :
  forall c c' r,
    all_net c' ->
    chan_concat c c' = Some r ->
    all_net r.
Proof.
  intros c c' r Han Hcc b t o Hin.
  destruct (chan_concat_buf_in _ _ _ _ _ _ Hcc Hin) as [b' H].
  exact (Han _ _ _ H).
Qed.

(* Concatenation of  wrap_loc β (TLoc l)  with an all-net channel is
   *independent* of l: only the buffer contents and structure matter. *)
Lemma chan_concat_wrap_loc_indep :
  forall β l l' c,
    all_net c ->
    chan_concat (wrap_loc β (TLoc l)) c = chan_concat (wrap_loc β (TLoc l')) c.
Proof.
  induction β as [b | β1 IH1 β2 IH2]; intros l l' c Han.
  - simpl. destruct c as [|ca cb|b0 t0 o0]; try reflexivity.
    destruct t0 as [l0|].
    + exfalso. assert (H : buf_in b0 (TLoc l0) o0 (CBuf b0 (TLoc l0) o0)) by (exists []; reflexivity).
      apply Han in H. discriminate H.
    + reflexivity.
  - simpl. destruct c as [|ca cb|b0 t0 o0]; try reflexivity.
    rewrite (IH1 l l' ca (all_net_pair_l _ _ Han)).
    rewrite (IH2 l l' cb (all_net_pair_r _ _ Han)).
    reflexivity.
Qed.

(* ============================================================================
   12. Relabeling one buffer commutes with ⧺.
   ========================================================================== *)

(* Relabeling one buffer of c2 (location t to t') commutes with ⧺ onto c2,
   provided the value appended at that buffer combines with t'. *)
Lemma chan_concat_relabel_at_path :
  forall π c1 c2 c2' b t t' o r1,
    chan_get c2 π = Some (CBuf b t o) ->
    chan_set c2 π (CBuf b t' o) = Some c2' ->
    chan_concat c1 c2 = Some r1 ->
    (forall b1 t1 o1, chan_get c1 π = Some (CBuf b1 t1 o1) -> ctype_combine t1 t' = Some t') ->
    exists r1' B,
      chan_concat c1 c2' = Some r1' /\
      chan_get r1 π = Some (CBuf B t o) /\
      chan_set r1 π (CBuf B t' o) = Some r1'.
Proof.
  induction π as [|d π' IH]; intros c1 c2 c2' b t t' o r1 Hg2 Hs2 Hcc Hcomb.
  - simpl in Hg2, Hs2. inversion Hg2; subst c2. inversion Hs2; subst c2'.
    destruct c1 as [|c1a c1b|b1 t1 o1]; simpl in Hcc; try discriminate.
    + inversion Hcc; subst r1. exists (CBuf b t' o), b. auto.
    + destruct (ctype_combine t1 t) as [tt|] eqn:Ect; try discriminate.
      inversion Hcc; subst r1. rewrite (ctype_combine_r _ _ _ Ect).
      exists (CBuf (b1 ++ b) t' o), (b1 ++ b).
      simpl. rewrite (Hcomb b1 t1 o1 eq_refl). auto.
  - destruct c2 as [|c2a c2b|? ? ?]; try (simpl in Hg2; destruct d; discriminate).
    destruct d; simpl in Hg2, Hs2.
    + destruct (chan_set c2a π' (CBuf b t' o)) as [c2a'|] eqn:Eset; try discriminate.
      inversion Hs2; subst c2'. clear Hs2.
      destruct c1 as [|c1a c1b|? ? ?]; simpl in Hcc; try discriminate.
      * inversion Hcc; subst r1. exists (CPair c2a' c2b), b. simpl. rewrite Eset. auto.
      * destruct (chan_concat c1a c2a) as [r1a|] eqn:Ea; try discriminate.
        destruct (chan_concat c1b c2b) as [r1b|] eqn:Eb; try discriminate.
        inversion Hcc; subst r1.
        destruct (IH c1a c2a c2a' b t t' o r1a Hg2 Eset Ea (fun b1 t1 o1 H => Hcomb b1 t1 o1 H))
          as [r1a' [B [Hcc_a' [Hgr Hsr]]]].
        exists (CPair r1a' r1b), B. simpl. rewrite Hcc_a', Eb, Hsr. auto.
    + destruct (chan_set c2b π' (CBuf b t' o)) as [c2b'|] eqn:Eset; try discriminate.
      inversion Hs2; subst c2'. clear Hs2.
      destruct c1 as [|c1a c1b|? ? ?]; simpl in Hcc; try discriminate.
      * inversion Hcc; subst r1. exists (CPair c2a c2b'), b. simpl. rewrite Eset. auto.
      * destruct (chan_concat c1a c2a) as [r1a|] eqn:Ea; try discriminate.
        destruct (chan_concat c1b c2b) as [r1b|] eqn:Eb; try discriminate.
        inversion Hcc; subst r1.
        destruct (IH c1b c2b c2b' b t t' o r1b Hg2 Eset Eb (fun b1 t1 o1 H => Hcomb b1 t1 o1 H))
          as [r1b' [B [Hcc_b' [Hgr Hsr]]]].
        exists (CPair r1a r1b'), B. simpl. rewrite Ea, Hcc_b', Hsr. auto.
Qed.

(* L2N's TLoc l → TNet: anything combines with TNet. *)
Lemma chan_concat_TLoc_TNet_at_path :
  forall π c1 c2 c2' b l o r1,
    chan_get c2 π = Some (CBuf b (TLoc l) o) ->
    chan_set c2 π (CBuf b TNet o) = Some c2' ->
    chan_concat c1 c2 = Some r1 ->
    exists r1' B,
      chan_concat c1 c2' = Some r1' /\
      chan_get r1 π = Some (CBuf B (TLoc l) o) /\
      chan_set r1 π (CBuf B TNet o) = Some r1'.
Proof.
  intros π c1 c2 c2' b l o r1 Hg Hs Hcc.
  eapply chan_concat_relabel_at_path; eauto.
  intros b1 [l1|] o1 _; reflexivity.
Qed.

(* LNL's TNet → TLoc l: the appended value must come from l. *)
Lemma chan_concat_TNet_TLoc_at_path :
  forall π c1 c2 c2' b l o r1,
    chan_get c2 π = Some (CBuf b TNet o) ->
    chan_set c2 π (CBuf b (TLoc l) o) = Some c2' ->
    chan_concat c1 c2 = Some r1 ->
    (forall b' t' o', chan_get c1 π = Some (CBuf b' t' o') -> t' = TLoc l) ->
    exists r1' B,
      chan_concat c1 c2' = Some r1' /\
      chan_get r1 π = Some (CBuf B TNet o) /\
      chan_set r1 π (CBuf B (TLoc l) o) = Some r1'.
Proof.
  intros π c1 c2 c2' b l o r1 Hg Hs Hcc Hloc.
  eapply chan_concat_relabel_at_path; eauto.
  intros b1 t1 o1 Hg1. rewrite (Hloc _ _ _ Hg1). simpl.
  destruct (Loc_eq_dec l l) as [_|E]; [reflexivity | exfalso; apply E; reflexivity].
Qed.

(* ============================================================================
   13. Steps of subexpressions.
   ========================================================================== *)

Lemma prog_step_inv :
  forall e c e_out c_out,
    prog_step (e, c) (e_out, c_out) ->
    exists c_inner, step e e_out c_inner /\ chan_concat c_inner c = Some c_out.
Proof.
  intros e c e_out c_out H.
  inversion H; subst.
  exists c'. split; assumption.
Qed.

(* Form of an operator step. *)
Lemma step_EOp_form :
  forall c o l e_out c_out,
    step (EOp c o l) e_out c_out ->
    exists π v t ord b1 b2 c' o' β,
       chan_get c π = Some (CBuf (b1 ++ v :: b2) t ord) /\
       (ord = Ordered -> b2 = []) /\
       (t = TLoc l \/ t = TNet) /\
       chan_set c π (CBuf (b1 ++ b2) t ord) = Some c' /\
       apply_op o π v = (o', β) /\
       e_out = EOp c' o' l /\
       c_out = wrap_loc β (TLoc l).
Proof.
  intros c o l e_out c_out H.
  inversion H; subst.
  exists π, v, t, ord, b1, b2, c', o', β. repeat split; assumption.
Qed.

Lemma step_EPar_form :
  forall e1 e2 e_out c_out,
    step (EPar e1 e2) e_out c_out ->
    (exists e1' c1', step e1 e1' c1' /\
                     e_out = EPar e1' e2 /\ c_out = CPair c1' CEps)
    \/
    (exists e2' c2', step e2 e2' c2' /\
                     e_out = EPar e1 e2' /\ c_out = CPair CEps c2').
Proof.
  intros e1 e2 e_out c_out H.
  inversion H; subst.
  - left.  repeat eexists; eauto.
  - right. repeat eexists; eauto.
Qed.

Lemma step_ESeq_form :
  forall e1 e2 e_out c_out,
    step (ESeq e1 e2) e_out c_out ->
    (exists e1' cc e2', step e1 e1' cc /\
                       feed cc e2 = Some e2' /\
                       e_out = ESeq e1' e2' /\ c_out = CEps)
    \/
    (exists e2' cc, step e2 e2' cc /\
                   e_out = ESeq e1 e2' /\ c_out = cc).
Proof.
  intros e1 e2 e_out c_out H.
  inversion H; subst.
  - left.  repeat eexists; eauto.
  - right. repeat eexists; eauto.
Qed.

(* If feed c e succeeds, the channel can be combined with e's input channel. *)
Lemma feed_inp_chan_concat :
  forall e c e' c_in,
    feed c e = Some e' ->
    inp_chan e = Some c_in ->
    exists r, chan_concat c c_in = Some r.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l]; intros c e_out c_in Hfeed Hinp.
  - (* EPar *)
    simpl in Hfeed, Hinp.
    destruct c as [|c1 c2|? ? ?]; try discriminate.
    + (* c = CEps: feed CEps (EPar) = Some (EPar e1 e2), chan_concat CEps _ = Some _ *)
      simpl. eexists; reflexivity.
    + destruct (feed c1 e1) as [e1'|] eqn:Ef1; try discriminate.
      destruct (feed c2 e2) as [e2'|] eqn:Ef2; try discriminate.
      destruct (inp_chan e1) as [ci1|] eqn:Ei1; try discriminate.
      destruct (inp_chan e2) as [ci2|] eqn:Ei2; try discriminate.
      inversion Hinp; subst.
      destruct (IH1 c1 e1' ci1 Ef1 eq_refl) as [r1 Hr1].
      destruct (IH2 c2 e2' ci2 Ef2 eq_refl) as [r2 Hr2].
      simpl. rewrite Hr1. rewrite Hr2. exists (CPair r1 r2). reflexivity.
  - (* ESeq *)
    simpl in Hfeed, Hinp.
    destruct (feed c e1) as [e1'|] eqn:Ef; try discriminate.
    eapply IH1; eauto.
  - (* EOp *)
    simpl in Hfeed, Hinp.
    inversion Hinp; subst.
    destruct (chan_concat c c_in) as [r|] eqn:Ecc; try discriminate.
    exists r. reflexivity.
Qed.

(* Feeding CEps is the identity on any well-formed expression. *)
Lemma feed_CEps_identity : forall e, feed CEps e = Some e.
Proof.
  induction e; simpl.
  - reflexivity.
  - rewrite IHe1. reflexivity.
  - reflexivity.
Qed.

(* inp_chan of the feed result equals chan_concat of feed-c with original inp_chan. *)
Lemma inp_chan_after_feed :
  forall e c e' c_in r,
    feed c e = Some e' ->
    inp_chan e = Some c_in ->
    chan_concat c c_in = Some r ->
    inp_chan e' = Some r.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l]; intros c e_r c_in r Hf Hinp Hcc.
  - (* EPar *)
    simpl in Hinp, Hf.
    destruct (inp_chan e1) as [ci1|] eqn:Ei1; try discriminate.
    destruct (inp_chan e2) as [ci2|] eqn:Ei2; try discriminate.
    inversion Hinp; subst c_in. clear Hinp.
    destruct c as [|ca cb|? ? ?]; try discriminate.
    + (* c = CEps: feed/inp/concat all identity-like *)
      inversion Hf; subst e_r.
      simpl. rewrite Ei1, Ei2.
      simpl in Hcc. inversion Hcc; subst r. reflexivity.
    + destruct (feed ca e1) as [e1_r|] eqn:Ff1; try discriminate.
      destruct (feed cb e2) as [e2_r|] eqn:Ff2; try discriminate.
      inversion Hf; subst e_r.
      simpl in Hcc.
      destruct (chan_concat ca ci1) as [ra|] eqn:Ea; try discriminate.
      destruct (chan_concat cb ci2) as [rb|] eqn:Eb; try discriminate.
      inversion Hcc; subst r.
      simpl.
      erewrite IH1; [|exact Ff1|reflexivity|exact Ea].
      erewrite IH2; [|exact Ff2|reflexivity|exact Eb].
      reflexivity.
  - (* ESeq *)
    simpl in Hinp, Hf.
    destruct (feed c e1) as [e1_r|] eqn:Ff; try discriminate.
    inversion Hf; subst e_r.
    simpl. eapply IH1; eauto.
  - (* EOp *)
    simpl in Hinp, Hf.
    inversion Hinp; subst c_in. clear Hinp.
    destruct (chan_concat c c0) as [r0|] eqn:Eo; try discriminate.
    inversion Hf; subst e_r.
    inversion Hcc; subst r. simpl. reflexivity.
Qed.

(* When e steps, its input channel may change: either it stays the same (e.g.,
   S_Seq_R where the step is on the right side) or one buffer is shortened by
   a same-type chan_set (e.g., S_Op extracts a value from a buffer). *)
Lemma step_inp_chan :
  forall e e_s c_out,
    step e e_s c_out ->
    forall c_in,
      inp_chan e = Some c_in ->
      exists c_in_new,
        inp_chan e_s = Some c_in_new /\
        (c_in_new = c_in \/
         exists π b b' t o,
           chan_get c_in π = Some (CBuf b t o) /\
           chan_set c_in π (CBuf b' t o) = Some c_in_new).
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l];
    intros e_s c_out Hstep c_in Hinp.
  - (* EPar e1 e2: step rules are S_Par_L and S_Par_R *)
    simpl in Hinp.
    destruct (inp_chan e1) as [ci1|] eqn:Ei1; try discriminate.
    destruct (inp_chan e2) as [ci2|] eqn:Ei2; try discriminate.
    inversion Hinp; subst c_in. clear Hinp.
    apply step_EPar_form in Hstep.
    destruct Hstep as [[e1_s' [c1' [Hs1 [Heq_e Heq_c]]]] |
                       [e2_s' [c2' [Hs2 [Heq_e Heq_c]]]]];
      subst e_s c_out.
    + (* S_Par_L *)
      destruct (IH1 _ _ Hs1 ci1 eq_refl) as [ci1' [Hinp_e1' Heq_or]].
      exists (CPair ci1' ci2). split.
      * simpl. rewrite Hinp_e1'. rewrite Ei2. reflexivity.
      * destruct Heq_or as [Heq|[π [b [b' [t [o [Hg Hs]]]]]]].
        -- subst ci1'. left. reflexivity.
        -- right. exists (DL :: π), b, b', t, o. split.
           ++ simpl. exact Hg.
           ++ simpl. rewrite Hs. reflexivity.
    + (* S_Par_R *)
      destruct (IH2 _ _ Hs2 ci2 eq_refl) as [ci2' [Hinp_e2' Heq_or]].
      exists (CPair ci1 ci2'). split.
      * simpl. rewrite Ei1. rewrite Hinp_e2'. reflexivity.
      * destruct Heq_or as [Heq|[π [b [b' [t [o [Hg Hs]]]]]]].
        -- subst ci2'. left. reflexivity.
        -- right. exists (DR :: π), b, b', t, o. split.
           ++ simpl. exact Hg.
           ++ simpl. rewrite Hs. reflexivity.
  - (* ESeq e1 e2: step rules are S_Seq_L and S_Seq_R *)
    simpl in Hinp.
    apply step_ESeq_form in Hstep.
    destruct Hstep as [[e1_s' [cc [e2_s' [Hs1 [Hfeed [Heq_e Heq_c]]]]]] |
                       [e2_s' [cc [Hs2 [Heq_e Heq_c]]]]];
      subst e_s c_out.
    + (* S_Seq_L: e_s' = ESeq e1_s' e2_s', e1 stepped *)
      destruct (IH1 _ _ Hs1 c_in Hinp) as [ci1' [Hinp_e1' Heq_or]].
      exists ci1'. split.
      * simpl. exact Hinp_e1'.
      * exact Heq_or.
    + (* S_Seq_R: e_s' = ESeq e1 e2_s', e1 unchanged, inp_chan only depends on e1 *)
      exists c_in. split.
      * simpl. exact Hinp.
      * left. reflexivity.
  - (* EOp c0 o l: one input buffer shortened *)
    simpl in Hinp. inversion Hinp; subst c_in. clear Hinp.
    apply step_EOp_form in Hstep.
    destruct Hstep as (π & v & t & ord & b1 & b2 & c' & o' & β & Hg & Hfifo & Ht & Hs & Hap & Heq & Hci).
    subst e_s c_out.
    exists c'. split.
    + reflexivity.
    + right. exists π, (b1 ++ v :: b2), (b1 ++ b2), t, ord.
      split; [exact Hg | exact Hs].
Qed.

(* Feed preserves the location at any path where the source has an EOp. *)
Lemma feed_preserves_expr_get_loc :
  forall e c e_post,
    feed c e = Some e_post ->
    forall π c1 o1 l1,
      expr_get e π = Some (EOp c1 o1 l1) ->
      exists c1' o1', expr_get e_post π = Some (EOp c1' o1' l1).
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l];
    intros c e_post Hfeed π_x c1_x o1_x l1_x Hexpr.
  - (* EPar *)
    simpl in Hfeed.
    destruct c as [|c_a c_b|? ? ?]; try discriminate.
    + (* c = CEps: feed is identity *)
      inversion Hfeed; subst e_post. clear Hfeed.
      destruct π_x as [|d π']; simpl in Hexpr.
      * inversion Hexpr.
      * destruct d; simpl; exists c1_x, o1_x; exact Hexpr.
    + destruct (feed c_a e1) as [e1_post|] eqn:Ff1; try discriminate.
      destruct (feed c_b e2) as [e2_post|] eqn:Ff2; try discriminate.
      inversion Hfeed; subst e_post. clear Hfeed.
      destruct π_x as [|d π']; simpl in Hexpr.
      * inversion Hexpr.
      * destruct d.
        -- destruct (IH1 _ _ Ff1 π' c1_x o1_x l1_x Hexpr) as [c1' [o1' Hex']].
           exists c1', o1'. simpl. exact Hex'.
        -- destruct (IH2 _ _ Ff2 π' c1_x o1_x l1_x Hexpr) as [c1' [o1' Hex']].
           exists c1', o1'. simpl. exact Hex'.
  - (* ESeq *)
    destruct π_x as [|d π']; simpl in Hexpr.
    + inversion Hexpr.
    + destruct d; discriminate.
  - (* EOp *)
    simpl in Hfeed.
    destruct (chan_concat c c0) as [c_post|] eqn:Ecc; try discriminate.
    inversion Hfeed; subst e_post. clear Hfeed.
    destruct π_x as [|d π']; simpl in Hexpr.
    + inversion Hexpr; subst.
      exists c_post, o1_x. reflexivity.
    + destruct d; discriminate.
Qed.

(* If a step from e produces output channel c_out, and e has an EOp at path π
   with operator location l, then a CBuf appearing at path π in c_out has type
   TLoc l (i.e., matches the operator's location). *)
Lemma step_output_loc_at_path :
  forall e e_s c_out,
    step e e_s c_out ->
    forall π c1 o1 l1 b t ord,
      expr_get e π = Some (EOp c1 o1 l1) ->
      chan_get c_out π = Some (CBuf b t ord) ->
      t = TLoc l1.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l];
    intros e_s c_out Hstep π_x c1_x o1_x l1_x b_x t_x ord_x Hexpr Hchan.
  - (* e = EPar *)
    apply step_EPar_form in Hstep.
    destruct Hstep as [[e1_s [c1' [Hs1 [Heq_e Heq_c]]]] |
                       [e2_s [c2' [Hs2 [Heq_e Heq_c]]]]];
      subst e_s c_out.
    + destruct π_x as [|d π']; simpl in Hexpr.
      * inversion Hexpr.
      * destruct d.
        -- simpl in Hchan. eapply IH1; eauto.
        -- simpl in Hchan.
           destruct π' as [|d' π''].
           ++ inversion Hchan.
           ++ destruct d'; discriminate.
    + destruct π_x as [|d π']; simpl in Hexpr.
      * inversion Hexpr.
      * destruct d.
        -- simpl in Hchan.
           destruct π' as [|d' π''].
           ++ inversion Hchan.
           ++ destruct d'; discriminate.
        -- simpl in Hchan. eapply IH2; eauto.
  - (* e = ESeq *)
    apply step_ESeq_form in Hstep.
    destruct Hstep as [[e1_s [cc [e2_s [Hs1 [Hfeed [Heq_e Heq_c]]]]]] |
                       [e2_s [cc [Hs2 [Heq_e Heq_c]]]]];
      subst e_s c_out.
    + (* S_Seq_L: c_out = CEps *)
      destruct π_x as [|d π']; simpl in Hchan.
      * inversion Hchan.
      * destruct d; discriminate.
    + (* S_Seq_R *)
      destruct π_x as [|d π']; simpl in Hexpr.
      * inversion Hexpr.
      * destruct d; discriminate.
  - (* e = EOp *)
    destruct π_x as [|d π']; simpl in Hexpr; [|destruct d; discriminate].
    inversion Hexpr; subst c1_x o1_x l1_x.
    apply step_EOp_form in Hstep.
    destruct Hstep as (π_op & v & t_op & ord & b1 & b2 & c' & o' & β & Hg & Hfifo & Ht & Hs & Hap & Heq & Hci).
    subst e_s c_out.
    (* S_Op emits at the operator's location *)
    destruct β; simpl in Hchan.
    + inversion Hchan; subst. reflexivity.
    + inversion Hchan.
Qed.

(* Step preserves the location at any path where the source expression has an EOp. *)
Lemma step_preserves_expr_get_loc :
  forall e e_s c_out,
    step e e_s c_out ->
    forall π c1 o1 l1,
      expr_get e π = Some (EOp c1 o1 l1) ->
      exists c1' o1', expr_get e_s π = Some (EOp c1' o1' l1).
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l];
    intros e_s c_out Hstep π_x c1_x o1_x l1_x Hexpr.
  - (* EPar *)
    apply step_EPar_form in Hstep.
    destruct Hstep as [[e1_s [c1' [Hs1 [Heq_e Heq_c]]]] |
                       [e2_s [c2' [Hs2 [Heq_e Heq_c]]]]];
      subst e_s c_out.
    + destruct π_x as [|d π']; simpl in Hexpr.
      * inversion Hexpr.
      * destruct d.
        -- destruct (IH1 _ _ Hs1 π' c1_x o1_x l1_x Hexpr) as [c1'_x [o1'_x Hex']].
           exists c1'_x, o1'_x. simpl. exact Hex'.
        -- exists c1_x, o1_x. simpl. exact Hexpr.
    + destruct π_x as [|d π']; simpl in Hexpr.
      * inversion Hexpr.
      * destruct d.
        -- exists c1_x, o1_x. simpl. exact Hexpr.
        -- destruct (IH2 _ _ Hs2 π' c1_x o1_x l1_x Hexpr) as [c1'_x [o1'_x Hex']].
           exists c1'_x, o1'_x. simpl. exact Hex'.
  - (* ESeq *)
    destruct π_x as [|d π']; simpl in Hexpr.
    + inversion Hexpr.
    + destruct d; discriminate.
  - (* EOp *)
    destruct π_x as [|d π']; simpl in Hexpr; [|destruct d; discriminate].
    inversion Hexpr; subst.
    apply step_EOp_form in Hstep.
    destruct Hstep as (π_op & v & t_op & ord & b1 & b2 & c'_op & o' & β & Hg & Hfifo & Ht & Hs & Hap & Heq & Hci).
    subst e_s.
    exists c'_op, o'. reflexivity.
Qed.

(* ============================================================================
   14. Type-system support.

      Step outputs are tightly typed (step_at_loc), so they combine with the
      input channel of whatever they feed.
   ========================================================================== *)

(* Helper: β has shape κ ⇒ wrap_loc β (TLoc l) is chan_at_loc shape_to_type κ. *)
Lemma wrap_loc_at_loc :
  forall β κ l o,
    bt_shape β κ ->
    chan_at_loc (wrap_loc β (TLoc l)) (shape_to_type κ (TLoc l) o).
Proof.
  induction β as [b | b1 IH1 b2 IH2]; intros κ l o Hbs.
  - inversion Hbs; subst. simpl. apply CAL_CBuf.
  - inversion Hbs; subst. simpl. apply CAL_CPair; auto.
Qed.

(* Step outputs are tight (chan_at_loc) wrt e's type. *)
Lemma step_at_loc :
  forall e e_s c_step τ,
    step e e_s c_step ->
    wt_inner e τ ->
    chan_at_loc c_step τ.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c o l];
    intros e_s c_step τ Hstep Hwt.
  - (* EPar *)
    apply step_EPar_form in Hstep.
    destruct Hstep as [[e1_s [c1' [Hs1 [Heq1 Heq2]]]] |
                       [e2_s [c2' [Hs2 [Heq1 Heq2]]]]].
    + subst e_s c_step.
      inversion Hwt; subst.
      apply CAL_CPair; [eapply IH1; eauto | apply CAL_CEps].
    + subst e_s c_step.
      inversion Hwt; subst.
      apply CAL_CPair; [apply CAL_CEps | eapply IH2; eauto].
  - (* ESeq *)
    apply step_ESeq_form in Hstep.
    destruct Hstep as [[e1_s [c' [e2_post [Hs1 [Hfeed [Heq1 Heq2]]]]]] |
                       [e2_s [c'' [Hs2 [Heq1 Heq2]]]]].
    + subst e_s c_step. apply CAL_CEps.
    + subst e_s c_step.
      inversion Hwt; subst.
      eapply IH2; eauto.
  - (* EOp *)
    apply step_EOp_form in Hstep.
    destruct Hstep as (π & v & t & ord & b1 & b2 & c' & o' & β & Hg & Hfifo & Ht & Hs & Hap & Heq & Hci).
    subst e_s c_step.
    inversion Hwt; subst.
    apply wrap_loc_at_loc. eapply apply_op_shape; eauto.
Qed.

(* chan_at_loc + chan_sub_type against the same τ ⇒ chan_concat succeeds. *)
Lemma chan_at_loc_subtype_concat :
  forall c1 c2 τ,
    chan_at_loc c1 τ ->
    chan_sub_type c2 τ ->
    exists r, chan_concat c1 c2 = Some r.
Proof.
  intros c1 c2 τ Hcal. revert c2.
  induction Hcal as [τ | b1 t o1 o' | c1a c1b a b Ha IHa Hb IHb]; intros c2 Hcst.
  - (* c1 = CEps *)
    simpl. eexists; reflexivity.
  - (* c1 = CBuf b1 t o1, τ = TTLeaf t o' *)
    inversion Hcst as [b2 t2 t2' o2 o2' Hsl Hol Heq1 Heq2 | ]; subst.
    inversion Hsl as [tx | lx]; subst.
    + (* SL_Refl: t2 = t *)
      destruct t as [l|]; simpl.
      * destruct (Loc_eq_dec l l) as [_|Hne];
          [eexists; reflexivity | exfalso; congruence].
      * eexists; reflexivity.
    + (* SL_NL: t2 = TNet, t = TLoc lx *)
      simpl. eexists; reflexivity.
  - (* c1 = CPair c1a c1b, τ = TTPair a b *)
    inversion Hcst as [| c2a c2b a' b' Hca Hcb Heq1 Heq2]; subst.
    destruct (IHa _ Hca) as [ra Hra].
    destruct (IHb _ Hcb) as [rb Hrb].
    simpl. rewrite Hra, Hrb. eexists; reflexivity.
Qed.

(* Inversion lemma for wt_out on an ESeq. *)
Lemma wt_out_ESeq_inv :
  forall e1 e2 c_outer,
    wt_out (ESeq e1 e2) c_outer ->
    exists τ1 τ2 c_in,
      wt_inner e1 τ1 /\
      inp_chan e2 = Some c_in /\
      chan_sub_type c_in τ1 /\
      wt_inner e2 τ2 /\
      chan_sub_type c_outer τ2.
Proof.
  intros e1 e2 c_outer [τ [Hwt Hco]].
  inversion Hwt; subst.
  exists τ1, τ, c_in. repeat split; assumption.
Qed.

Lemma wt_out_Par_L_inv :
  forall e1 e2 c1 c2,
    wt_out (EPar e1 e2) (CPair c1 c2) ->
    wt_out e1 c1.
Proof.
  intros e1 e2 c1 c2 [τ [Hwt Hco]].
  inversion Hwt; subst.
  inversion Hco as [| ca cb a b Hca Hcb Heq1 Heq2]; subst.
  exists τ1. split; assumption.
Qed.

Lemma wt_out_Par_R_inv :
  forall e1 e2 c1 c2,
    wt_out (EPar e1 e2) (CPair c1 c2) ->
    wt_out e2 c2.
Proof.
  intros e1 e2 c1 c2 [τ [Hwt Hco]].
  inversion Hwt; subst.
  inversion Hco as [| ca cb a b Hca Hcb Heq1 Heq2]; subst.
  exists τ2. split; assumption.
Qed.

Lemma wt_out_Seq_L_inv :
  forall e1 e2 c_outer c_in,
    wt_out (ESeq e1 e2) c_outer ->
    inp_chan e2 = Some c_in ->
    wt_out e1 c_in.
Proof.
  intros e1 e2 c_outer c_in_arg [τ [Hwt Hco]] Hinp.
  inversion Hwt; subst.
  match goal with H : inp_chan e2 = Some _ |- _ => rewrite Hinp in H; inversion H; subst c_in_arg end.
  exists τ1. split; assumption.
Qed.

Lemma wt_out_Seq_R_inv :
  forall e1 e2 c_outer,
    wt_out (ESeq e1 e2) c_outer ->
    wt_out e2 c_outer.
Proof.
  intros e1 e2 c_outer [τ [Hwt Hco]].
  inversion Hwt; subst.
  exists τ. split; assumption.
Qed.

(* ============================================================================
   15. Type-preservation and Trans-Type-preservation (Doc.tex theorems).
   ========================================================================== *)

(* The apply_op runtime preserves the operator signature.  This is a
   side-condition on apply_op: stepping cannot change an operator's I/O
   shapes (only its identity / consumed-buffer state).  Used in S_Op. *)
Axiom apply_op_sig_preserved :
  forall o π v o' β,
    apply_op o π v = (o', β) ->
    Op_sig o' = Op_sig o.

(* Helper: type at a given path in a shape_to_type κ t. *)
Fixpoint type_at_path (π : Path) (τ : type_tree) : option type_tree :=
  match π with
  | [] => Some τ
  | DL :: π' => match τ with TTPair a _ => type_at_path π' a | _ => None end
  | DR :: π' => match τ with TTPair _ b => type_at_path π' b | _ => None end
  end.

(* Replacing one buffer keeps a channel sub-typed, if the new location still
   fits the type at that path. *)
Lemma chan_sub_type_relabel :
  forall c π b t o b' t' c' τ,
    chan_sub_type c τ ->
    chan_get c π = Some (CBuf b t o) ->
    chan_set c π (CBuf b' t' o) = Some c' ->
    (forall t0 o0, type_at_path π τ = Some (TTLeaf t0 o0) -> sub_leaf t t0 -> sub_leaf t' t0) ->
    chan_sub_type c' τ.
Proof.
  intros c π. revert c.
  induction π as [|[|] π IH]; intros c b t o b' t' c' τ Hsub Hget Hset Hcond.
  - simpl in Hget, Hset. inversion Hget; subst c. inversion Hset; subst c'.
    inversion Hsub as [b0 t0 t0' o0 o0' Hsl Hol Heq1 Heq2 |]; subst.
    constructor; [exact (Hcond t0' o0' eq_refl Hsl) | exact Hol].
  - simpl in Hget, Hset. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set ca π (CBuf b' t' o)) as [ca'|] eqn:Eset; try discriminate.
    inversion Hset; subst c'. clear Hset.
    inversion Hsub as [| ca0 cb0 a0 b0 Ha Hb Heq1 Heq2]; subst.
    constructor; [eapply IH; [exact Ha | exact Hget | exact Eset | exact Hcond] | assumption].
  - simpl in Hget, Hset. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set cb π (CBuf b' t' o)) as [cb'|] eqn:Eset; try discriminate.
    inversion Hset; subst c'. clear Hset.
    inversion Hsub as [| ca0 cb0 a0 b0 Ha Hb Heq1 Heq2]; subst.
    constructor; [assumption | eapply IH; [exact Hb | exact Hget | exact Eset | exact Hcond]].
Qed.

(* A buffer's contents can change freely. *)
Lemma chan_sub_type_chan_set_same_t :
  forall c π b1 b2 t o c' τ,
    chan_sub_type c τ ->
    chan_get c π = Some (CBuf b1 t o) ->
    chan_set c π (CBuf b2 t o) = Some c' ->
    chan_sub_type c' τ.
Proof. intros. eapply chan_sub_type_relabel; eauto. Qed.

(* L2N's TLoc l → TNet: a network buffer fits wherever a local one did. *)
Lemma chan_sub_type_TLoc_to_TNet :
  forall c π b l o c' τ,
    chan_sub_type c τ ->
    chan_get c π = Some (CBuf b (TLoc l) o) ->
    chan_set c π (CBuf b TNet o) = Some c' ->
    chan_sub_type c' τ.
Proof.
  intros. eapply chan_sub_type_relabel; eauto.
  intros t0 o0 _ Hsl. inversion Hsl; subst. constructor.
Qed.

(* If c is all_net and chan_sub_type c τ with τ = shape_to_type κ (TLoc l),
   then chan_sub_type c (shape_to_type κ (TLoc l')) for any l'. *)
Lemma chan_sub_type_change_loc_all_net :
  forall c κ l l' o,
    chan_sub_type c (shape_to_type κ (TLoc l) o) ->
    all_net c ->
    chan_sub_type c (shape_to_type κ (TLoc l') o).
Proof.
  intros c κ l l' o Hsub Han.
  revert c Hsub Han.
  induction κ as [|κ1 IH1 κ2 IH2]; intros c Hsub Han; simpl in *.
  - inversion Hsub as [b t t' o0 o0' Hsl Hol Heq1 Heq2|]; subst.
    inversion Hsl; subst.
    + (* SL_Refl: c = CBuf b (TLoc l) _, contradicting all_net c *)
      exfalso.
      assert (Hbi : buf_in b (TLoc l) o0 (CBuf b (TLoc l) o0)).
      { exists nil. reflexivity. }
      pose proof (Han b (TLoc l) o0 Hbi) as Hcontra. discriminate.
    + constructor; [apply SL_NL | exact Hol].
  - inversion Hsub as [| ca cb a b Ha Hb Heq1 Heq2]; subst.
    constructor.
    + apply IH1; [exact Ha | eapply all_net_pair_l; exact Han].
    + apply IH2; [exact Hb | eapply all_net_pair_r; exact Han].
Qed.



(* From wt_inner e τ + expr_get e π = Some (EOp c1 o1 l),
   we get that τ at path π is shape_to_type κ2 (TLoc l) where Op_sig o1 = (_, κ2). *)
Lemma wt_inner_expr_get_EOp :
  forall e τ π c1 o1 l,
    wt_inner e τ ->
    expr_get e π = Some (EOp c1 o1 l) ->
    exists κ1 κ2 ord,
      Op_sig o1 = (κ1, κ2) /\
      type_at_path π τ = Some (shape_to_type κ2 (TLoc l) ord).
Proof.
  intros e. induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c o lx];
    intros τ π c1 o1 l Hwt Hg.
  - inversion Hwt as [e1x e2x τ1 τ2 Hw1 Hw2 | |]; subst.
    destruct π as [|d π']; simpl in Hg.
    + discriminate.
    + destruct d.
      * destruct (IH1 _ _ _ _ _ Hw1 Hg) as [κ1 [κ2 [ord [Heq Hpath]]]].
        exists κ1, κ2, ord. split; [exact Heq | simpl; exact Hpath].
      * destruct (IH2 _ _ _ _ _ Hw2 Hg) as [κ1 [κ2 [ord [Heq Hpath]]]].
        exists κ1, κ2, ord. split; [exact Heq | simpl; exact Hpath].
  - inversion Hwt; subst.
    destruct π as [|d π']; simpl in Hg.
    + discriminate.
    + destruct d; discriminate.
  - inversion Hwt as [| |cx ox lxx κ1 κ2 Hosig Hcsub Hfifo]; subst.
    destruct π as [|d π']; simpl in Hg.
    + inversion Hg; subst.
      exists κ1, κ2, (out_ord o1 c1). split; [assumption | reflexivity].
    + destruct d; discriminate.
Qed.

(* chan_sub_type at a path. *)
(* The "rigidity" form: chan_sub_type forces the type structure at path π
   to be TTLeaf whenever the channel has CBuf at π. *)
Lemma chan_sub_type_get_buf_path :
  forall π c b t o τ,
    chan_sub_type c τ ->
    chan_get c π = Some (CBuf b t o) ->
    exists t' o', type_at_path π τ = Some (TTLeaf t' o') /\ sub_leaf t t' /\ ord_le o o'.
Proof.
  induction π as [|[|] π IH]; intros c b t o τ Hsub Hget.
  - simpl in Hget. inversion Hget; subst c.
    inversion Hsub as [b0 t0 t0' o0 o0' Hsl Hol Heq1 Heq2|]; subst.
    exists t0', o0'. auto.
  - simpl in *. destruct c as [|ca cb|? ? ?]; try discriminate.
    inversion Hsub as [| ca0 cb0 a0 b0 Ha Hb Heq1 Heq2]; subst.
    exact (IH _ _ _ _ _ Ha Hget).
  - simpl in *. destruct c as [|ca cb|? ? ?]; try discriminate.
    inversion Hsub as [| ca0 cb0 a0 b0 Ha Hb Heq1 Heq2]; subst.
    exact (IH _ _ _ _ _ Hb Hget).
Qed.

(* LNL's TNet → TLoc l, where τ's leaf at π is at l. *)
Lemma chan_sub_type_TNet_to_TLoc :
  forall c π b l o c' τ,
    chan_sub_type c τ ->
    chan_get c π = Some (CBuf b TNet o) ->
    chan_set c π (CBuf b (TLoc l) o) = Some c' ->
    (forall t' o', type_at_path π τ = Some (TTLeaf t' o') -> t' = TLoc l) ->
    chan_sub_type c' τ.
Proof.
  intros c π b l o c' τ Hsub Hg Hs Hpath. eapply chan_sub_type_relabel; eauto.
  intros t0 o0 Hp _. rewrite (Hpath _ _ Hp). constructor.
Qed.

(* Order labels are unchanged by a chan_set that keeps the buffer's order, and
   ⧺ keeps the right side's order labels; so is every operator's out_ord. *)
Lemma chan_all_ordered_set :
  forall π c b t o b' t' c',
    chan_get c π = Some (CBuf b t o) ->
    chan_set c π (CBuf b' t' o) = Some c' ->
    chan_all_ordered c' = chan_all_ordered c.
Proof.
  induction π as [|[|] π IH]; intros c b t o b' t' c' Hget Hset.
  - simpl in *. inversion Hget; subst. inversion Hset; subst. destruct o; reflexivity.
  - simpl in *. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set ca π (CBuf b' t' o)) as [ca'|] eqn:Eset; try discriminate.
    inversion Hset; subst. simpl. rewrite (IH ca b t o b' t' ca' Hget Eset). reflexivity.
  - simpl in *. destruct c as [|ca cb|? ? ?]; try discriminate.
    destruct (chan_set cb π (CBuf b' t' o)) as [cb'|] eqn:Eset; try discriminate.
    inversion Hset; subst. simpl. rewrite (IH cb b t o b' t' cb' Hget Eset). reflexivity.
Qed.

Lemma chan_all_ordered_concat :
  forall c1 c2 r, chan_concat c1 c2 = Some r -> chan_all_ordered r = chan_all_ordered c2.
Proof.
  induction c1 as [|c1a IHa c1b IHb|b1 t1 o1]; intros c2 r H.
  - simpl in H. inversion H; reflexivity.
  - destruct c2 as [|c2a c2b|? ? ?]; simpl in H; try discriminate.
    destruct (chan_concat c1a c2a) as [ra|] eqn:Ea; try discriminate.
    destruct (chan_concat c1b c2b) as [rb|] eqn:Eb; try discriminate.
    inversion H; subst. simpl. rewrite (IHa _ _ Ea), (IHb _ _ Eb). reflexivity.
  - destruct c2 as [|? ?|b2 t2 o2]; simpl in H; try discriminate.
    destruct (ctype_combine t1 t2); try discriminate.
    inversion H; subst. reflexivity.
Qed.

(* The relabeling rules keep typing. *)
Lemma relabel_inp :
  forall e e' c_outer, relabel e e' c_outer ->
    forall ci τ,
      inp_chan e = Some ci ->
      chan_sub_type ci τ ->
      exists ci', inp_chan e' = Some ci' /\ chan_sub_type ci' τ.
Proof.
  intros e e' c_outer0 Hrl. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1];
    intros ci τ Hinp Hsub; simpl in Hinp.
  - (* L2N *)
    inversion Hinp; subst ci. clear Hinp.
    exists c'. split.
    + simpl. reflexivity.
    + eapply chan_sub_type_TLoc_to_TNet; eauto.
  - (* NLN: c unchanged *)
    inversion Hinp; subst ci. clear Hinp.
    exists c. split; [simpl; reflexivity | exact Hsub].
  - (* LNL: ESeq's inp_chan = inp_chan e1, unchanged *)
    exists ci. split; [simpl; exact Hinp | exact Hsub].
Qed.

Lemma relabel_wt :
  forall e e' c_outer, relabel e e' c_outer -> wt_out e c_outer -> wt_out e' c_outer.
Proof.
  intros e e' c_outer0 Hrl. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1]; intros Hwt.
  - (* L2N: relabel (EOp c o l) (EOp c' o l) c_outer — only a location
       label changes, so the order labels and hence out_ord are unchanged *)
    destruct Hwt as [τ [Hwt_op Hco]].
    inversion Hwt_op as [| | c_x o_x l_x κ1 κ2 Hosig Hcsub Hfifo]; subst.
    pose proof (chan_all_ordered_set _ _ _ _ _ _ _ _ H H0) as Hord.
    exists (shape_to_type κ2 (TLoc l) (out_ord o c')). split.
    + eapply W_Op_t; [exact Hosig | |].
      * eapply chan_sub_type_TLoc_to_TNet; eauto.
      * rewrite Hord. exact Hfifo.
    + unfold out_ord. rewrite Hord. exact Hco.

  - (* NLN: relabel (EOp c o l) (EOp c o l') c_outer *)
    destruct Hwt as [τ [Hwt_op Hco]].
    inversion Hwt_op as [| | c_x o_x l_x κ1 κ2 Hosig Hcsub Hfifo]; subst.
    exists (shape_to_type κ2 (TLoc l') (out_ord o c)). split.
    + eapply W_Op_t; [exact Hosig | | exact Hfifo].
      eapply chan_sub_type_change_loc_all_net; eauto.
    + eapply chan_sub_type_change_loc_all_net; eauto.

  - (* LNL: relabel (ESeq e1 (EOp c o2 l)) (ESeq e1 (EOp c' o2 l)) c_outer
       (chan_get c π = TNet, chan_set c π (TLoc l) = c', e1 has EOp with location l at π) *)
    destruct Hwt as [τ [Hwt_s Hco]].
    inversion Hwt_s as [|e1_x e2_x τ1 τ2 c_in_x Hwte1 Hinpe2 Hsub_in Hwte2|]; subst.
    simpl in Hinpe2. inversion Hinpe2; subst c_in_x. clear Hinpe2.
    inversion Hwte2 as [| | c_xy o_xy l_xy κ1_op κ2_op Hosig_op Hcsub_op Hfifo_op]; subst.
    pose proof (chan_all_ordered_set _ _ _ _ _ _ _ _ H0 H1) as Hord.
    exists (shape_to_type κ2_op (TLoc l) (out_ord o2 c')). split.
    + pose proof (wt_inner_expr_get_EOp _ _ _ _ _ _ Hwte1 H) as
        [κ1_e1 [κ2_e1 [ord_e1 [Hosig_e1 Hpath_τ1]]]].
      eapply W_Seq_t; [exact Hwte1 | simpl; reflexivity | |].
      * eapply chan_sub_type_TNet_to_TLoc; [exact Hsub_in | exact H0 | exact H1 |].
        intros t' o' Hp. rewrite Hpath_τ1 in Hp. inversion Hp as [Hpeq].
        destruct κ2_e1; simpl in Hpeq; [|discriminate].
        inversion Hpeq; reflexivity.
      * eapply W_Op_t; [exact Hosig_op | | rewrite Hord; exact Hfifo_op].
        eapply chan_sub_type_TNet_to_TLoc; [exact Hcsub_op | exact H0 | exact H1 |].
        intros t' o' Hp. clear -Hp.
        revert κ1_op Hp.
        induction π as [|[|] π IH]; intros κ1_op Hp.
        -- destruct κ1_op; simpl in Hp; try discriminate.
           inversion Hp. reflexivity.
        -- destruct κ1_op; simpl in Hp; try discriminate. eapply IH; eauto.
        -- destruct κ1_op; simpl in Hp; try discriminate. eapply IH; eauto.
    + unfold out_ord. rewrite Hord. exact Hco.
Qed.

(* Typing under transformations.  A base rewrite must keep the rewritten part
   well typed in its context, and its input channel typable against what feeds
   it. *)
Definition typing_conds (B : Expr -> Expr -> Chan -> Prop) : Prop :=
  (forall e e' c ci τ,
     B e e' c -> inp_chan e = Some ci -> chan_sub_type ci τ ->
     exists ci', inp_chan e' = Some ci' /\ chan_sub_type ci' τ) /\
  (forall e e' c, B e e' c -> wt_out e c -> wt_out e' c).

Lemma relabel_typing : typing_conds relabel.
Proof. split; [intros e e' c ci τ H; eapply relabel_inp; eauto | exact relabel_wt]. Qed.

Lemma typing_conds_ext : forall B, typing_conds B -> typing_conds (rw_ext B).
Proof.
  intros B [Hi Hw]. split.
  - intros e e' c ci τ [H|H]; [eapply relabel_inp; eauto | eapply Hi; eauto].
  - intros e e' c [H|H]; [eapply relabel_wt; eauto | eapply Hw; eauto].
Qed.

Section TransTyping.

Variable B : Expr -> Expr -> Chan -> Prop.
Hypothesis HB : typing_conds B.

Lemma transform_preserves_inp_chan_sub_type :
  forall e e' c_outer,
    transform B e e' c_outer ->
    forall ci τ,
      inp_chan e = Some ci ->
      chan_sub_type ci τ ->
      exists ci', inp_chan e' = Some ci' /\ chan_sub_type ci' τ.
Proof.
  intros e e' c_outer Htr.
  induction Htr; intros ci τ Hinp Hsub; simpl in Hinp.
  - (* T_Par_L *)
    destruct (inp_chan e1) as [ci1|] eqn:E1; try discriminate.
    destruct (inp_chan e2) as [ci2|] eqn:E2; try discriminate.
    inversion Hinp; subst ci. clear Hinp.
    inversion Hsub as [| ca cb a b Ha Hb Heq1 Heq2]; subst.
    specialize (IHHtr ci1 a eq_refl Ha) as [ci1' [Hinp1' Hsub1']].
    exists (CPair ci1' ci2). split.
    + simpl. rewrite Hinp1', E2. reflexivity.
    + constructor; assumption.
  - (* T_Par_R *)
    destruct (inp_chan e1) as [ci1|] eqn:E1; try discriminate.
    destruct (inp_chan e2) as [ci2|] eqn:E2; try discriminate.
    inversion Hinp; subst ci. clear Hinp.
    inversion Hsub as [| ca cb a b Ha Hb Heq1 Heq2]; subst.
    specialize (IHHtr ci2 b eq_refl Hb) as [ci2' [Hinp2' Hsub2']].
    exists (CPair ci1 ci2'). split.
    + simpl. rewrite E1, Hinp2'. reflexivity.
    + constructor; assumption.
  - (* T_Seq_L *)
    destruct (IHHtr _ _ Hinp Hsub) as [ci' [Hinp' Hsub']].
    exists ci'. split; [simpl; exact Hinp' | exact Hsub'].
  - (* T_Seq_R *)
    exists ci. split; [simpl; exact Hinp | exact Hsub].
  - (* T_Base *)
    eapply (proj1 HB); eauto.
Qed.

(* Trans-Type-preservation (Doc.tex §"Transformations"). *)
Theorem Trans_Type_preservation :
  forall e e' c,
    wt_out e c ->
    transform B e e' c ->
    wt_out e' c.
Proof.
  intros e e' c Hwt Htr. revert Hwt.
  induction Htr; intros Hwt.

  - (* T_Par_L: transform0 (EPar e1 e2) (EPar e1' e2) (CPair c1 c2) *)
    destruct Hwt as [τ [Hwt_p Hco]].
    inversion Hwt_p as [e1_x e2_x τ1 τ2 Hwte1 Hwte2 | |]; subst.
    inversion Hco as [| ca cb a b Ha Hb Heq1 Heq2]; subst.
    assert (Hwt1 : wt_out e1 c1).
    { exists τ1. split; assumption. }
    apply IHHtr in Hwt1.
    destruct Hwt1 as [τ1' [Hwte1' Hco1']].
    exists (TTPair τ1' τ2). split.
    + apply W_Par_t; assumption.
    + apply CST_CPair; assumption.

  - (* T_Par_R: symmetric *)
    destruct Hwt as [τ [Hwt_p Hco]].
    inversion Hwt_p as [e1_x e2_x τ1 τ2 Hwte1 Hwte2 | |]; subst.
    inversion Hco as [| ca cb a b Ha Hb Heq1 Heq2]; subst.
    assert (Hwt2 : wt_out e2 c2).
    { exists τ2. split; assumption. }
    apply IHHtr in Hwt2.
    destruct Hwt2 as [τ2' [Hwte2' Hco2']].
    exists (TTPair τ1 τ2'). split.
    + apply W_Par_t; assumption.
    + apply CST_CPair; assumption.

  - (* T_Seq_L: transform0 (ESeq e1 e2) (ESeq e1' e2) c_outer *)
    destruct Hwt as [τ [Hwt_s Hco]].
    inversion Hwt_s as [|e1_x e2_x τ1 τ2 c_in_x Hwte1 Hinpe2 Hsub_in Hwte2|]; subst.
    rewrite H in Hinpe2. inversion Hinpe2; subst c_in_x. clear Hinpe2.
    assert (Hwt1 : wt_out e1 c_in).
    { exists τ1. split; assumption. }
    apply IHHtr in Hwt1.
    destruct Hwt1 as [τ1' [Hwte1' Hco1']].
    exists τ. split; [|assumption].
    eapply W_Seq_t; [exact Hwte1' | exact H | exact Hco1' | exact Hwte2].

  - (* T_Seq_R: transform0 (ESeq e1 e2) (ESeq e1 e2') c_outer *)
    destruct Hwt as [τ [Hwt_s Hco]].
    inversion Hwt_s as [|e1_x e2_x τ1 τ2 c_in Hwte1 Hinpe2 Hsub_in Hwte2|]; subst.
    assert (Hwt2 : wt_out e2 c_outer).
    { exists τ. split; assumption. }
    apply IHHtr in Hwt2.
    destruct Hwt2 as [τ2' [Hwte2' Hco2']].
    (* From transform0 e2 e2' c_outer + inp_chan e2 = Some c_in + chan_sub_type c_in τ1,
       we derive inp_chan e2' = Some c_in' with chan_sub_type c_in' τ1. *)
    pose proof (transform_preserves_inp_chan_sub_type _ _ _ Htr _ _ Hinpe2 Hsub_in) as
      [c_in' [Hinpe2' Hsub_in']].
    exists τ2'. split.
    + eapply W_Seq_t; [exact Hwte1 | exact Hinpe2' | exact Hsub_in' | exact Hwte2'].
    + exact Hco2'.

  - (* T_Base *)
    eapply (proj2 HB); eauto.
Qed.

End TransTyping.




(* chan_concat with an at_loc-typed first channel and a sub-typed second channel
   produces a sub-typed result against the second channel's type. *)
Lemma chan_concat_at_loc_sub_type :
  forall c1 c2 r τ1 τ2,
    chan_at_loc c1 τ1 ->
    chan_sub_type c2 τ2 ->
    chan_concat c1 c2 = Some r ->
    chan_sub_type r τ2.
Proof.
  (* ⧺ keeps c2's labels, so r is typed like c2. *)
  intros c1 c2 r τ1 τ2 _ Hsub. revert c1 r.
  induction Hsub as [b2 t2 t2' o2 o2' Hsl Hol | c2a c2b a b Ha IHa Hb IHb]; intros c1 r Hcc.
  - destruct c1 as [|? ?|b1 t1 o1]; simpl in Hcc; try discriminate.
    + inversion Hcc; subst. constructor; assumption.
    + destruct (ctype_combine t1 t2) as [t|] eqn:Ect; try discriminate.
      inversion Hcc; subst. rewrite (ctype_combine_r _ _ _ Ect). constructor; assumption.
  - destruct c1 as [|c1a c1b|? ? ?]; simpl in Hcc; try discriminate.
    + inversion Hcc; subst. constructor; assumption.
    + destruct (chan_concat c1a c2a) as [ra|] eqn:Era; try discriminate.
      destruct (chan_concat c1b c2b) as [rb|] eqn:Erb; try discriminate.
      inversion Hcc; subst. constructor; eauto.
Qed.

(* Stepping an operator keeps its ordering interface and signature. *)
Lemma apply_op_iface_preserved :
  forall o π v o' β,
    apply_op o π v = (o', β) ->
    (forall c, out_ord o' c = out_ord o c) /\ reads_fifo o' = reads_fifo o /\ Op_sig o' = Op_sig o.
Proof.
  intros o π v o' β Hap.
  pose proof (apply_op_mode_preserved _ _ _ _ _ Hap) as Hm.
  unfold out_ord, reads_fifo. rewrite Hm.
  split; [reflexivity | split; [reflexivity | eapply apply_op_sig_preserved; eauto]].
Qed.








(* For our usage we only need feed of a step output (chan_at_loc-typed) into a
   wt_inner expression preserves wt_inner. *)
Lemma feed_at_loc_preserves_wt_inner :
  forall e c c_in e' τ τ_inp,
    feed c e = Some e' ->
    wt_inner e τ ->
    inp_chan e = Some c_in ->
    chan_sub_type c_in τ_inp ->
    chan_at_loc c τ_inp ->
    wt_inner e' τ.
Proof.
  intros e. induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l];
    intros c c_in e_out τ τ_inp Hfeed Hwt Hinp Hsub Hcal; simpl in Hfeed, Hinp.
  - (* EPar *)
    destruct c as [|ca cb|? ? ?]; try discriminate.
    + (* c = CEps: feed is identity, wt_inner unchanged *)
      inversion Hfeed; subst e_out. exact Hwt.
    + destruct (feed ca e1) as [e1'|] eqn:Ef1; try discriminate.
      destruct (feed cb e2) as [e2'|] eqn:Ef2; try discriminate.
      inversion Hfeed; subst e_out. clear Hfeed.
      inversion Hwt as [e1_x e2_x τ1 τ2 Hwte1 Hwte2 | | ]; subst.
      destruct (inp_chan e1) as [ci1|] eqn:E1; try discriminate.
      destruct (inp_chan e2) as [ci2|] eqn:E2; try discriminate.
      inversion Hinp; subst c_in. clear Hinp.
      inversion Hsub as [| ca0 cb0 a0 b0 Ha Hb Heq1 Heq2]; subst.
      inversion Hcal as [ ? | ? ? | c_x c_y a_x b_x Hca Hcb ]; subst.
      constructor; [eapply IH1 | eapply IH2]; eauto.
  - (* ESeq *)
    destruct (feed c e1) as [e1'|] eqn:Ef1; try discriminate.
    inversion Hfeed; subst e_out. clear Hfeed.
    inversion Hwt as [|e1_x e2_x τ1 τ2 c_inner Hwte1 Hinpe2 Hsub_in Hwte2|]; subst.
    eapply W_Seq_t with (c_in := c_inner); [|exact Hinpe2 | exact Hsub_in | exact Hwte2].
    eapply IH1; eauto.
  - (* EOp: ⧺ keeps the order labels, hence out_ord *)
    destruct (chan_concat c c0) as [c'|] eqn:Ecc; try discriminate.
    inversion Hfeed; subst e_out. clear Hfeed.
    inversion Hwt as [| |c_x o_x l_x κ1 κ2 Hosig Hcsub Hfifo]; subst.
    inversion Hinp; subst c_in. clear Hinp.
    pose proof (chan_all_ordered_concat _ _ _ Ecc) as Hord.
    replace (out_ord o c0) with (out_ord o c') by (unfold out_ord; rewrite Hord; reflexivity).
    eapply W_Op_t; [exact Hosig | | rewrite Hord; exact Hfifo].
    eapply chan_concat_at_loc_sub_type; [exact Hcal | exact Hcsub | exact Ecc].
Qed.

(* Step preserves wt_inner.  This is essentially step_at_loc combined with
   feed/chan_set preservation lemmas. *)
Lemma step_preserves_wt_inner :
  forall e e_s c',
    step e e_s c' ->
    forall τ, wt_inner e τ -> wt_inner e_s τ.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c o l];
    intros e_s c' Hstep τ Hwt.
  - (* EPar *)
    apply step_EPar_form in Hstep.
    destruct Hstep as [[e1_s [c1' [Hs1 [Heq1 Heq2]]]] |
                       [e2_s [c2' [Hs2 [Heq1 Heq2]]]]].
    + subst e_s. inversion Hwt as [ex1 ex2 τ1 τ2 Hw1 Hw2 | | ]; subst.
      constructor; [eapply IH1; eauto | assumption].
    + subst e_s. inversion Hwt as [ex1 ex2 τ1 τ2 Hw1 Hw2 | | ]; subst.
      constructor; [assumption | eapply IH2; eauto].
  - (* ESeq *)
    apply step_ESeq_form in Hstep.
    destruct Hstep as [[e1_s [c'' [e2_post [Hs1 [Hfeed [Heq1 Heq2]]]]]] |
                       [e2_s [c''' [Hs2 [Heq1 Heq2]]]]].
    + subst e_s.
      inversion Hwt as [|ex1 ex2 τ1 τ2 c_in Hw1 Hinp Hsub Hw2|]; subst.
      destruct (feed_inp_chan_concat _ _ _ _ Hfeed Hinp) as [r Hr].
      pose proof (inp_chan_after_feed _ _ _ _ _ Hfeed Hinp Hr) as Hinp_post.
      pose proof (step_at_loc _ _ _ τ1 Hs1 Hw1) as Hcal.
      eapply W_Seq_t with (c_in := r).
      * eapply IH1; eauto.
      * exact Hinp_post.
      * eapply chan_concat_at_loc_sub_type;
          [exact Hcal | exact Hsub | exact Hr].
      * eapply feed_at_loc_preserves_wt_inner;
          [exact Hfeed | exact Hw2 | exact Hinp | exact Hsub | exact Hcal].
    + subst e_s.
      inversion Hwt as [|ex1 ex2 τ1 τ2 c_in Hw1 Hinp Hsub Hw2|]; subst.
      (* Need wt_inner (ESeq e1 e2_s) τ.
         Build via W_Seq_t with new c_in_new from step_inp_chan. *)
      pose proof (step_inp_chan _ _ _ Hs2 _ Hinp) as [c_in_new [Hinp_new Hor]].
      eapply W_Seq_t with (c_in := c_in_new).
      * exact Hw1.
      * exact Hinp_new.
      * destruct Hor as [Heq | [πs [b [b' [tS [o_set [Hg_in Hs_in]]]]]]].
        -- subst c_in_new. exact Hsub.
        -- eapply chan_sub_type_chan_set_same_t; eauto.
      * eapply IH2; eauto.
  - (* EOp *)
    apply step_EOp_form in Hstep.
    destruct Hstep as (π & v & t & ord & b1 & b2 & c'_post & o' & β & Hg & Hfifo & Ht & Hs & Hap & Heq & Hci).
    subst e_s.
    inversion Hwt as [| | cx ox lx κ1 κ2 Hosig Hcsub Hfifo_t]; subst.
    (* the operator keeps its signature and modes, and its input its labels *)
    destruct (apply_op_iface_preserved _ _ _ _ _ Hap) as [Hout [Hrf Hosig']].
    pose proof (chan_all_ordered_set _ _ _ _ _ _ _ _ Hg Hs) as Hord.
    replace (out_ord o c) with (out_ord o' c'_post)
      by (rewrite Hout; unfold out_ord; rewrite Hord; reflexivity).
    eapply W_Op_t with (κ1 := κ1) (κ2 := κ2).
    + rewrite Hosig'. exact Hosig.
    + eapply chan_sub_type_chan_set_same_t; [exact Hcsub | exact Hg | exact Hs].
    + rewrite Hrf, Hord. exact Hfifo_t.
Qed.

Theorem Type_preservation :
  forall e c e_s c',
    wt_out e c ->
    prog_step (e, c) (e_s, c') ->
    wt_out e_s c'.
Proof.
  intros e c e_s c' Hwt Hstep.
  apply prog_step_inv in Hstep.
  destruct Hstep as [c_inner [Hstep_inner Hcc]].
  destruct Hwt as [τ [Hwte Hco]].
  pose proof (step_at_loc _ _ _ τ Hstep_inner Hwte) as Hcal.
  pose proof (chan_concat_at_loc_sub_type _ _ _ _ _ Hcal Hco Hcc) as Hco'.
  exists τ. split; [|exact Hco'].
  eapply step_preserves_wt_inner; eauto.
Qed.

(* Non-empty list decomposes as snoc:  any non-[] list is some b1 ++ [v]. *)
Lemma exists_snoc :
  forall (A : Type) (x : A) (xs : list A),
    exists b1 v, x :: xs = b1 ++ [v].
Proof.
  intros A x xs. revert x. induction xs as [|y ys IH]; intros x.
  - exists [], x. reflexivity.
  - destruct (IH y) as [b1 [v Heq]].
    exists (x :: b1), v. simpl. f_equal. exact Heq.
Qed.

(* Find a non-empty buffer in a channel (and its path).  Used by Progress to
   produce the witness for S_Op. *)
Fixpoint find_nonempty (c : Chan) : option (Path * Buffer) :=
  match c with
  | CEps => None
  | CBuf b _ _ => match b with [] => None | _ => Some ([], b) end
  | CPair c1 c2 =>
      match find_nonempty c1 with
      | Some (π, b) => Some (DL :: π, b)
      | None =>
          match find_nonempty c2 with
          | Some (π, b) => Some (DR :: π, b)
          | None => None
          end
      end
  end.

Lemma find_nonempty_get :
  forall c π b,
    find_nonempty c = Some (π, b) ->
    (exists t o, chan_get c π = Some (CBuf b t o)) /\ b <> [].
Proof.
  induction c as [|c1 IH1 c2 IH2|b' t' o']; intros π b H; simpl in H.
  - discriminate.
  - destruct (find_nonempty c1) as [[π1 b1]|] eqn:E1.
    + inversion H; subst. exact (IH1 _ _ eq_refl).
    + destruct (find_nonempty c2) as [[π2 b2]|] eqn:E2; try discriminate.
      inversion H; subst. exact (IH2 _ _ eq_refl).
  - destruct b' as [|v vs] eqn:Eb; try discriminate.
    inversion H; subst.
    split; [exists t', o'; reflexivity | intro Hcontra; discriminate].
Qed.

Lemma find_nonempty_None_all_empty :
  forall c, find_nonempty c = None ->
    forall b t o, buf_in b t o c -> b = [].
Proof.
  induction c as [|c1 IH1 c2 IH2|b' t' o']; intros H b t o [π Hget]; simpl in *.
  - destruct π; simpl in Hget; [discriminate | destruct d; discriminate].
  - destruct (find_nonempty c1) as [[π1 b1]|] eqn:E1; try discriminate.
    destruct (find_nonempty c2) as [[π2 b2]|] eqn:E2; try discriminate.
    destruct π as [|d π']; simpl in Hget; try discriminate.
    destruct d.
    + eapply IH1; eauto. exists π'. exact Hget.
    + eapply IH2; eauto. exists π'. exact Hget.
  - destruct π as [|d π']; simpl in Hget.
    + inversion Hget; subst.
      destruct b as [|v vs] eqn:Eb; try discriminate. reflexivity.
    + destruct d; discriminate.
Qed.



(* In a sub-typed channel, a CBuf leaf has type sub-leaf of τ's leaf — so
   either TLoc l (matching) or TNet (the SL_NL case). *)
Lemma chan_sub_type_leaf_disjunction :
  forall c π b t o κ l ord,
    chan_sub_type c (shape_to_type κ (TLoc l) ord) ->
    chan_get c π = Some (CBuf b t o) ->
    t = TLoc l \/ t = TNet.
Proof.
  intros c π b t o κ l ord Hsub Hget.
  pose proof (chan_sub_type_get_buf_path _ _ _ _ _ _ Hsub Hget) as
    [t' [o' [Hpath [Hsl _]]]].
  (* shape_to_type κ (TLoc l) ord at any path is either a leaf at TLoc l or a
     pair; Hpath says it's a leaf, so its location is TLoc l. *)
  assert (Ht' : t' = TLoc l).
  { clear -Hpath.
    revert κ Hpath. induction π as [|[|] π IH]; intros κ Hpath.
    - destruct κ; simpl in Hpath; try discriminate.
      inversion Hpath. reflexivity.
    - destruct κ; simpl in Hpath; try discriminate. eapply IH; eauto.
    - destruct κ; simpl in Hpath; try discriminate. eapply IH; eauto. }
  subst t'.
  inversion Hsl; [left; reflexivity | right; reflexivity].
Qed.

(* feed succeeds when the feed channel is chan_at_loc-typed and the
   expression's input is sub-typed against the same τ. *)
Lemma feed_succeeds_at_loc :
  forall e c τ c_in,
    chan_at_loc c τ ->
    inp_chan e = Some c_in ->
    chan_sub_type c_in τ ->
    exists e', feed c e = Some e'.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l];
    intros c τ c_in Hcal Hinp Hsub; simpl in Hinp.
  - (* EPar *)
    destruct (inp_chan e1) as [ci1|] eqn:E1; try discriminate.
    destruct (inp_chan e2) as [ci2|] eqn:E2; try discriminate.
    inversion Hinp; subst c_in. clear Hinp.
    inversion Hsub as [| c_x c_y a b Ha Hb Heq1 Heq2]; subst.
    inversion Hcal as [τ0 | b0 t0 o0 o0' | c_xx c_yy a_x b_x Hcx Hcy]; subst.
    + (* c = CEps: feed CEps (EPar) = Some _ *)
      exists (EPar e1 e2). simpl. reflexivity.
    + (* c = CPair *)
      destruct (IH1 _ _ _ Hcx eq_refl Ha) as [e1' Hf1].
      destruct (IH2 _ _ _ Hcy eq_refl Hb) as [e2' Hf2].
      simpl. rewrite Hf1, Hf2. eexists; reflexivity.
  - (* ESeq *)
    destruct (IH1 _ _ _ Hcal Hinp Hsub) as [e1' Hf1].
    simpl. rewrite Hf1. eexists; reflexivity.
  - (* EOp *)
    inversion Hinp; subst c_in. clear Hinp.
    destruct (chan_at_loc_subtype_concat _ _ _ Hcal Hsub) as [r Hr].
    simpl. rewrite Hr. eexists; reflexivity.
Qed.

(* Progress on inner expressions: well-typed inner exprs are either done
   or step (with chan_at_loc output). *)
Lemma progress_inner :
  forall e τ,
    wt_inner e τ ->
    done e \/ exists e_s c_inner, step e e_s c_inner /\ chan_at_loc c_inner τ.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l];
    intros τ Hwt.

  - (* EPar *)
    inversion Hwt as [e1x e2x τ1 τ2 Hw1 Hw2 | |]; subst.
    destruct (IH1 _ Hw1) as [Hdone1 | [e1_s [c1' [Hs1 Hcal1]]]];
    destruct (IH2 _ Hw2) as [Hdone2 | [e2_s [c2' [Hs2 Hcal2]]]].
    + (* both done *) left. apply F_Par; assumption.
    + (* e1 done, e2 steps *) right. exists (EPar e1 e2_s), (CPair CEps c2').
      split; [apply S_Par_R; exact Hs2 | apply CAL_CPair; [apply CAL_CEps | exact Hcal2]].
    + (* e1 steps, e2 done *) right. exists (EPar e1_s e2), (CPair c1' CEps).
      split; [apply S_Par_L; exact Hs1 | apply CAL_CPair; [exact Hcal1 | apply CAL_CEps]].
    + (* both step: pick e1 *) right. exists (EPar e1_s e2), (CPair c1' CEps).
      split; [apply S_Par_L; exact Hs1 | apply CAL_CPair; [exact Hcal1 | apply CAL_CEps]].

  - (* ESeq *)
    inversion Hwt as [|e1x e2x τ1 τ2 c_in Hw1 Hinp Hsub Hw2|]; subst.
    destruct (IH1 _ Hw1) as [Hdone1 | [e1_s [c1' [Hs1 Hcal1]]]];
    destruct (IH2 _ Hw2) as [Hdone2 | [e2_s [c2' [Hs2 Hcal2]]]].
    + left. apply F_Seq; assumption.
    + right. exists (ESeq e1 e2_s), c2'. split; [apply S_Seq_R; exact Hs2 | exact Hcal2].
    + (* e1 steps, e2 done.  S_Seq_L needs feed c1' e2 to succeed. *)
      destruct (feed_succeeds_at_loc e2 c1' τ1 c_in Hcal1 Hinp Hsub) as [e2_post Hfeed].
      right. exists (ESeq e1_s e2_post), CEps.
      split; [eapply S_Seq_L; [exact Hs1 | exact Hfeed] | apply CAL_CEps].
    + (* both step: pick e1 *)
      destruct (feed_succeeds_at_loc e2 c1' τ1 c_in Hcal1 Hinp Hsub) as [e2_post Hfeed].
      right. exists (ESeq e1_s e2_post), CEps.
      split; [eapply S_Seq_L; [exact Hs1 | exact Hfeed] | apply CAL_CEps].

  - (* EOp *)
    inversion Hwt as [| | cx ox lx κ1 κ2 Hosig Hcsub Hfifo]; subst.
    destruct (find_nonempty c0) as [[π b]|] eqn:Efn.
    + (* non-empty input buffer found: S_Op consumes its oldest value *)
      right.
      destruct (find_nonempty_get _ _ _ Efn) as [[t [ord Hget]] Hne_b].
      destruct b as [|v0 rest]; [exfalso; apply Hne_b; reflexivity|].
      clear Hne_b.
      destruct (exists_snoc _ v0 rest) as [b1 [v Heq]].
      rewrite Heq in Hget.
      destruct (chan_set_at_buf_exists _ _ _ _ _ (CBuf (b1 ++ []) t ord) Hget) as [c' Hset].
      destruct (apply_op o π v) as [o' β] eqn:Hap.
      exists (EOp c' o' l), (wrap_loc β (TLoc l)).
      split.
      * eapply S_Op with (b2 := []);
          [exact Hget | intros _; reflexivity
          | eapply chan_sub_type_leaf_disjunction; eauto
          | exact Hset | exact Hap].
      * apply wrap_loc_at_loc. eapply apply_op_shape; eauto.
    + (* all buffers empty: done via F_Op *)
      left. apply F_Op. eapply find_nonempty_None_all_empty. exact Efn.
Qed.

(* Progress (Doc.tex §"Type system"): a well-typed configuration either is
   done or steps. *)
Theorem Progress :
  forall e c,
    wt_out e c ->
    done e \/ exists e_s c', prog_step (e, c) (e_s, c').
Proof.
  intros e c [τ [Hwte Hco]].
  destruct (progress_inner e τ Hwte) as [Hdone | [e_s [c_inner [Hstep Hcal]]]].
  - left. exact Hdone.
  - right.
    destruct (chan_at_loc_subtype_concat _ _ _ Hcal Hco) as [c'' Hcc].
    exists e_s, c''. eapply P_Step; eauto.
Qed.

(* ============================================================================
   16. Simulations.

       A base rewrite need not be lockstep: one step of one side may be
       matched by several steps of the other.  Its soundness follows from two
       simulations of its own:
         - forward (original → rewritten): a relation Bf ⊇ B in which both
           sides have the same output context;
         - reverse (rewritten → original): a relation Br ⊇ B in which the
           original side may run ahead of the rewritten one, so the two output
           contexts may differ (lag_rel below).
       Final outputs then agree up to chan_equiv: equal on Ordered buffers,
       equal as bags on Unordered ones.  The congruence rules are handled
       here once, for every base rewrite.
   ========================================================================== *)

From Stdlib Require Import Permutation.

(* The observables. *)
Definition reaches_out (e : Expr) (c : Chan) (c_final : Chan) : Prop :=
  exists e_final, prog_star (e, c) (e_final, c_final).

Definition fin_output (e : Expr) (c : Chan) (c_final : Chan) : Prop :=
  exists e_final, prog_star (e, c) (e_final, c_final) /\ done e_final.

(* Final outputs compared by their labels. *)
Inductive chan_equiv : Chan -> Chan -> Prop :=
  | CQ_Eps   : chan_equiv CEps CEps
  | CQ_Ord   : forall b t, chan_equiv (CBuf b t Ordered) (CBuf b t Ordered)
  | CQ_Unord : forall b b' t, Permutation b b' -> chan_equiv (CBuf b t Unordered) (CBuf b' t Unordered)
  | CQ_Pair  : forall a b a' b', chan_equiv a a' -> chan_equiv b b' ->
      chan_equiv (CPair a b) (CPair a' b').

Lemma chan_equiv_refl : forall c, chan_equiv c c.
Proof.
  induction c as [|a IHa b IHb|bf t [|]]; constructor; auto.
Qed.

Lemma chan_equiv_empty :
  forall c c', chan_equiv c c' ->
    (forall b t o, buf_in b t o c' -> b = []) -> (forall b t o, buf_in b t o c -> b = []).
Proof.
  intros c c' H. induction H; intros Hall b0 t0 o0 Hin.
  - destruct Hin as [[|[|] π] Hg]; discriminate.
  - destruct (buf_in_cbuf _ _ _ _ _ _ Hin) as (-> & -> & ->).
    eapply Hall. exists []. reflexivity.
  - destruct (buf_in_cbuf _ _ _ _ _ _ Hin) as (-> & -> & ->).
    assert (Hb' : b' = []) by (eapply Hall; exists []; reflexivity). subst b'.
    apply Permutation_nil. apply Permutation_sym. exact H.
  - destruct (buf_in_pair _ _ _ _ _ Hin) as [Ha|Hb].
    + eapply IHchan_equiv1; [|exact Ha].
      intros x y z [π Hg]. eapply Hall. exists (DL :: π). exact Hg.
    + eapply IHchan_equiv2; [|exact Hb].
      intros x y z [π Hg]. eapply Hall. exists (DR :: π). exact Hg.
Qed.

(* Removing one value v from the buffer at π, labeled (t, o), obeying its order. *)
Definition chan_remove (c : Chan) (π : Path) (v : Val) (t : CType) (o : Order) (c' : Chan) : Prop :=
  exists A B, chan_get c π = Some (CBuf (A ++ v :: B) t o) /\ (o = Ordered -> B = []) /\
              chan_set c π (CBuf (A ++ B) t o) = Some c'.

Lemma chan_remove_pair_l :
  forall a b π v t o c', chan_remove (CPair a b) (DL :: π) v t o c' ->
    exists a', chan_remove a π v t o a' /\ c' = CPair a' b.
Proof.
  intros a b π v t o c' (A & B & Hg & Ho & Hs). simpl in Hg, Hs.
  destruct (chan_set a π (CBuf (A ++ B) t o)) as [a'|] eqn:E; try discriminate.
  inversion Hs; subst. exists a'. split; [exists A, B; auto | reflexivity].
Qed.

Lemma chan_remove_pair_r :
  forall a b π v t o c', chan_remove (CPair a b) (DR :: π) v t o c' ->
    exists b', chan_remove b π v t o b' /\ c' = CPair a b'.
Proof.
  intros a b π v t o c' (A & B & Hg & Ho & Hs). simpl in Hg, Hs.
  destruct (chan_set b π (CBuf (A ++ B) t o)) as [b'|] eqn:E; try discriminate.
  inversion Hs; subst. exists b'. split; [exists A, B; auto | reflexivity].
Qed.

Lemma chan_remove_into_l :
  forall a b π v t o a', chan_remove a π v t o a' -> chan_remove (CPair a b) (DL :: π) v t o (CPair a' b).
Proof. intros a b π v t o a' (A & B & Hg & Ho & Hs). exists A, B. simpl. rewrite Hs. auto. Qed.

Lemma chan_remove_into_r :
  forall a b π v t o b', chan_remove b π v t o b' -> chan_remove (CPair a b) (DR :: π) v t o (CPair a b').
Proof. intros a b π v t o b' (A & B & Hg & Ho & Hs). exists A, B. simpl. rewrite Hs. auto. Qed.

(* ---------------- Replacing an expression's input channel ---------------- *)

Fixpoint set_inp (e : Expr) (c : Chan) : option Expr :=
  match e with
  | EPar e1 e2 =>
      match c with
      | CPair c1 c2 =>
          match set_inp e1 c1, set_inp e2 c2 with
          | Some e1', Some e2' => Some (EPar e1' e2')
          | _, _ => None
          end
      | _ => None
      end
  | ESeq e1 e2 =>
      match set_inp e1 c with
      | Some e1' => Some (ESeq e1' e2)
      | None => None
      end
  | EOp _ o l => Some (EOp c o l)
  end.

Lemma set_inp_self : forall e ci, inp_chan e = Some ci -> set_inp e ci = Some e.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l]; intros ci H; simpl in *.
  - destruct (inp_chan e1) as [c1|] eqn:E1; try discriminate.
    destruct (inp_chan e2) as [c2|] eqn:E2; try discriminate.
    inversion H; subst. rewrite (IH1 _ eq_refl), (IH2 _ eq_refl). reflexivity.
  - rewrite (IH1 _ H). reflexivity.
  - inversion H; subst. reflexivity.
Qed.

Lemma set_inp_inp : forall e r e', set_inp e r = Some e' -> inp_chan e' = Some r.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l]; intros r e' H; simpl in H.
  - destruct r as [|r1 r2|? ? ?]; try discriminate.
    destruct (set_inp e1 r1) as [a|] eqn:E1; try discriminate.
    destruct (set_inp e2 r2) as [b|] eqn:E2; try discriminate.
    inversion H; subst. simpl. rewrite (IH1 _ _ E1), (IH2 _ _ E2). reflexivity.
  - destruct (set_inp e1 r) as [a|] eqn:E1; try discriminate.
    inversion H; subst. simpl. exact (IH1 _ _ E1).
  - inversion H; subst. reflexivity.
Qed.

Lemma set_inp_twice :
  forall e r e' r' e'', set_inp e r = Some e' -> set_inp e' r' = Some e'' -> set_inp e r' = Some e''.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l]; intros r e' r' e'' H H'; simpl in H.
  - destruct r as [|r1 r2|? ? ?]; try discriminate.
    destruct (set_inp e1 r1) as [a|] eqn:E1; try discriminate.
    destruct (set_inp e2 r2) as [b|] eqn:E2; try discriminate.
    inversion H; subst. simpl in H'.
    destruct r' as [|r1' r2'|? ? ?]; try discriminate.
    destruct (set_inp a r1') as [a'|] eqn:E1'; try discriminate.
    destruct (set_inp b r2') as [b'|] eqn:E2'; try discriminate.
    inversion H'; subst. simpl. rewrite (IH1 _ _ _ _ E1 E1'), (IH2 _ _ _ _ E2 E2'). reflexivity.
  - destruct (set_inp e1 r) as [a|] eqn:E1; try discriminate.
    inversion H; subst. simpl in H'.
    destruct (set_inp a r') as [a'|] eqn:E1'; try discriminate.
    inversion H'; subst. simpl. rewrite (IH1 _ _ _ _ E1 E1'). reflexivity.
  - inversion H; subst. simpl in H'. exact H'.
Qed.

(* Feeding f is setting the input to f ⧺ (current input). *)
Lemma feed_as_set_inp :
  forall e ci f r,
    inp_chan e = Some ci -> chan_concat f ci = Some r ->
    exists e', feed f e = Some e' /\ set_inp e r = Some e'.
Proof.
  induction e as [e1 IH1 e2 IH2 | e1 IH1 e2 IH2 | c0 o l]; intros ci f r Hinp Hcc; simpl in Hinp.
  - destruct (inp_chan e1) as [c1|] eqn:E1; try discriminate.
    destruct (inp_chan e2) as [c2|] eqn:E2; try discriminate.
    inversion Hinp; subst ci. clear Hinp.
    destruct f as [|f1 f2|? ? ?]; simpl in Hcc; try discriminate.
    + inversion Hcc; subst r. exists (EPar e1 e2). split; [reflexivity|].
      apply set_inp_self. simpl. rewrite E1, E2. reflexivity.
    + destruct (chan_concat f1 c1) as [r1|] eqn:Er1; try discriminate.
      destruct (chan_concat f2 c2) as [r2|] eqn:Er2; try discriminate.
      inversion Hcc; subst r.
      destruct (IH1 _ _ _ eq_refl Er1) as [a [Hfa Hsa]].
      destruct (IH2 _ _ _ eq_refl Er2) as [b [Hfb Hsb]].
      exists (EPar a b). simpl. rewrite Hfa, Hfb, Hsa, Hsb. split; reflexivity.
  - destruct (IH1 _ _ _ Hinp Hcc) as [a [Hfa Hsa]].
    exists (ESeq a e2). simpl. rewrite Hfa, Hsa. split; reflexivity.
  - inversion Hinp; subst ci. exists (EOp r o l). simpl. rewrite Hcc. split; reflexivity.
Qed.

(* ---------------- Lifting runs of a part into its context ---------------- *)

Lemma prog_star_trans :
  forall x y z, prog_star x y -> prog_star y z -> prog_star x z.
Proof.
  intros x y z Hxy Hyz. induction Hxy; [exact Hyz | eapply PS_step; eauto].
Qed.

Lemma lift_par_L :
  forall e1 c1 e1' c1', prog_star (e1, c1) (e1', c1') ->
    forall e2 c2, prog_star (EPar e1 e2, CPair c1 c2) (EPar e1' e2, CPair c1' c2).
Proof.
  intros e1 c1 e1' c1' H. remember (e1, c1) as x. remember (e1', c1') as y.
  revert e1 c1 Heqx. induction H; intros e1 c1 Heqx e2 c2; subst.
  - inversion Heqx; subst. apply PS_refl.
  - destruct y as [ye yc].
    apply prog_step_inv in H. destruct H as [em [Hs Hcc]].
    eapply PS_step; [| eapply IHprog_star; eauto].
    eapply P_Step; [apply S_Par_L; exact Hs | simpl; rewrite Hcc; reflexivity].
Qed.

Lemma lift_par_R :
  forall e2 c2 e2' c2', prog_star (e2, c2) (e2', c2') ->
    forall e1 c1, prog_star (EPar e1 e2, CPair c1 c2) (EPar e1 e2', CPair c1 c2').
Proof.
  intros e2 c2 e2' c2' H. remember (e2, c2) as x. remember (e2', c2') as y.
  revert e2 c2 Heqx. induction H; intros e2 c2 Heqx e1 c1; subst.
  - inversion Heqx; subst. apply PS_refl.
  - destruct y as [ye yc].
    apply prog_step_inv in H. destruct H as [em [Hs Hcc]].
    eapply PS_step; [| eapply IHprog_star; eauto].
    eapply P_Step; [apply S_Par_R; exact Hs | simpl; rewrite Hcc; reflexivity].
Qed.

Lemma lift_seq_R :
  forall e2 c e2' c', prog_star (e2, c) (e2', c') ->
    forall e1, prog_star (ESeq e1 e2, c) (ESeq e1 e2', c').
Proof.
  intros e2 c e2' c' H. remember (e2, c) as x. remember (e2', c') as y.
  revert e2 c Heqx. induction H; intros e2 c Heqx e1; subst.
  - inversion Heqx; subst. apply PS_refl.
  - destruct y as [ye yc].
    apply prog_step_inv in H. destruct H as [em [Hs Hcc]].
    eapply PS_step; [| eapply IHprog_star; eauto].
    eapply P_Step; [apply S_Seq_R; exact Hs | exact Hcc].
Qed.

(* A run of e1 into the input channel of e2 is a run of e1 ; e2 that feeds e2. *)
Lemma lift_seq_L :
  forall e1 ci e1' ci', prog_star (e1, ci) (e1', ci') ->
    forall e2 c, inp_chan e2 = Some ci ->
      exists e2', set_inp e2 ci' = Some e2' /\ prog_star (ESeq e1 e2, c) (ESeq e1' e2', c).
Proof.
  intros e1 ci e1' ci' H. remember (e1, ci) as x. remember (e1', ci') as y.
  revert e1 ci Heqx. induction H; intros e1 ci Heqx e2 c Hinp; subst.
  - inversion Heqx; subst. exists e2. split; [apply set_inp_self; exact Hinp | apply PS_refl].
  - destruct y as [ye yc].
    apply prog_step_inv in H. destruct H as [em [Hs Hcc]].
    destruct (feed_as_set_inp _ _ _ _ Hinp Hcc) as [e2a [Hfa Hsa]].
    destruct (IHprog_star eq_refl ye yc eq_refl e2a c (set_inp_inp _ _ _ Hsa)) as [e2' [Hs2 Hss]].
    exists e2'. split; [eapply set_inp_twice; eauto|].
    eapply PS_step; [| exact Hss].
    eapply P_Step; [eapply S_Seq_L; [exact Hs | exact Hfa] | reflexivity].
Qed.

(* ---------------- Forward simulation ---------------- *)

Section Forward.

Variable Bf : Expr -> Expr -> Chan -> Prop.

Hypothesis Bf_step : forall e e' c es cs,
  Bf e e' c -> prog_step (e, c) (es, cs) ->
  exists e's, prog_star (e', c) (e's, cs) /\ Bf es e's cs.
Hypothesis Bf_feed : forall e e' c f ep,
  Bf e e' c -> feed f e = Some ep ->
  exists ep', feed f e' = Some ep' /\ Bf ep ep' c.
Hypothesis Bf_ctx : forall e e' c π b b' t o c',
  Bf e e' c -> chan_get c π = Some (CBuf b t o) ->
  chan_set c π (CBuf b' t o) = Some c' -> Bf e e' c'.
Hypothesis Bf_done : forall e e' c, Bf e e' c -> done e -> done e'.

Lemma fwd_ctx : forall e e' c π b b' t o c',
  transform Bf e e' c -> chan_get c π = Some (CBuf b t o) ->
  chan_set c π (CBuf b' t o) = Some c' -> transform Bf e e' c'.
Proof.
  intros e e' c π b b' t o c' H. revert π b b' t o c'.
  induction H; intros π b b' t o c' Hg Hs.
  - destruct π as [|[|] π]; simpl in Hg, Hs; try discriminate.
    + destruct (chan_set c1 π (CBuf b' t o)) as [c1'|] eqn:E; try discriminate.
      inversion Hs; subst. apply T_Par_L. eapply IHtransform; eauto.
    + destruct (chan_set c2 π (CBuf b' t o)) as [c2'|] eqn:E; try discriminate.
      inversion Hs; subst. apply T_Par_L. exact H.
  - destruct π as [|[|] π]; simpl in Hg, Hs; try discriminate.
    + destruct (chan_set c1 π (CBuf b' t o)) as [c1'|] eqn:E; try discriminate.
      inversion Hs; subst. apply T_Par_R. exact H.
    + destruct (chan_set c2 π (CBuf b' t o)) as [c2'|] eqn:E; try discriminate.
      inversion Hs; subst. apply T_Par_R. eapply IHtransform; eauto.
  - eapply T_Seq_L; eauto.
  - apply T_Seq_R. eapply IHtransform; eauto.
  - apply T_Base. eapply Bf_ctx; eauto.
Qed.

Lemma fwd_feed : forall e e' c f ep,
  transform Bf e e' c -> feed f e = Some ep ->
  exists ep', feed f e' = Some ep' /\ transform Bf ep ep' c.
Proof.
  intros e e' c f ep H. revert f ep.
  induction H; intros f ep Hf.
  - simpl in Hf. destruct f as [|fa fb|? ? ?]; try discriminate.
    + inversion Hf; subst. exists (EPar e1' e2). split; [reflexivity | apply T_Par_L; exact H].
    + destruct (feed fa e1) as [a|] eqn:Ea; try discriminate.
      destruct (feed fb e2) as [b|] eqn:Eb; try discriminate.
      inversion Hf; subst.
      destruct (IHtransform _ _ Ea) as [a' [Ha' Hr]].
      exists (EPar a' b). simpl. rewrite Ha', Eb. split; [reflexivity | apply T_Par_L; exact Hr].
  - simpl in Hf. destruct f as [|fa fb|? ? ?]; try discriminate.
    + inversion Hf; subst. exists (EPar e1 e2'). split; [reflexivity | apply T_Par_R; exact H].
    + destruct (feed fa e1) as [a|] eqn:Ea; try discriminate.
      destruct (feed fb e2) as [b|] eqn:Eb; try discriminate.
      inversion Hf; subst.
      destruct (IHtransform _ _ Eb) as [b' [Hb' Hr]].
      exists (EPar a b'). simpl. rewrite Ea, Hb'. split; [reflexivity | apply T_Par_R; exact Hr].
  - simpl in Hf. destruct (feed f e1) as [a|] eqn:Ea; try discriminate.
    inversion Hf; subst.
    destruct (IHtransform _ _ Ea) as [a' [Ha' Hr]].
    exists (ESeq a' e2). simpl. rewrite Ha'. split; [reflexivity | eapply T_Seq_L; eauto].
  - simpl in Hf. destruct (feed f e1) as [a|] eqn:Ea; try discriminate.
    inversion Hf; subst.
    exists (ESeq a e2'). simpl. rewrite Ea. split; [reflexivity | apply T_Seq_R; exact H].
  - destruct (Bf_feed _ _ _ _ _ H Hf) as [ep' [Hf' HB]]. exists ep'. split; [exact Hf' | apply T_Base; exact HB].
Qed.

Lemma fwd_sim : forall e e' c es cs,
  transform Bf e e' c -> prog_step (e, c) (es, cs) ->
  exists e's, prog_star (e', c) (e's, cs) /\ transform Bf es e's cs.
Proof.
  intros e e' c es cs H. revert es cs.
  induction H; intros es cs Hstep.
  - (* T_Par_L *)
    apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
    apply step_EPar_form in Hs.
    destruct Hs as [(e1s & em1 & Hs1 & -> & ->) | (e2s & em2 & Hs2 & -> & ->)];
      simpl in Hcc.
    + destruct (chan_concat em1 c1) as [c1s|] eqn:E1; try discriminate.
      inversion Hcc; subst cs.
      destruct (IHtransform e1s c1s (P_Step _ _ _ _ _ Hs1 E1)) as [e1's [Hss Hr]].
      exists (EPar e1's e2). split; [apply lift_par_L; exact Hss | apply T_Par_L; exact Hr].
    + destruct (chan_concat em2 c2) as [c2s|] eqn:E2; try discriminate.
      inversion Hcc; subst cs.
      exists (EPar e1' e2s). split; [|apply T_Par_L; exact H].
      eapply PS_step; [|apply PS_refl].
      eapply P_Step; [apply S_Par_R; exact Hs2 | simpl; rewrite E2; reflexivity].
  - (* T_Par_R *)
    apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
    apply step_EPar_form in Hs.
    destruct Hs as [(e1s & em1 & Hs1 & -> & ->) | (e2s & em2 & Hs2 & -> & ->)];
      simpl in Hcc.
    + destruct (chan_concat em1 c1) as [c1s|] eqn:E1; try discriminate.
      inversion Hcc; subst cs.
      exists (EPar e1s e2'). split; [|apply T_Par_R; exact H].
      eapply PS_step; [|apply PS_refl].
      eapply P_Step; [apply S_Par_L; exact Hs1 | simpl; rewrite E1; reflexivity].
    + destruct (chan_concat em2 c2) as [c2s|] eqn:E2; try discriminate.
      inversion Hcc; subst cs.
      destruct (IHtransform e2s c2s (P_Step _ _ _ _ _ Hs2 E2)) as [e2's [Hss Hr]].
      exists (EPar e1 e2's). split; [apply lift_par_R; exact Hss | apply T_Par_R; exact Hr].
  - (* T_Seq_L *)
    apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
    apply step_ESeq_form in Hs.
    destruct Hs as [(e1s & em1 & e2p & Hs1 & Hf & -> & ->) | (e2s & em2 & Hs2 & -> & ->)].
    + (* e1 steps and feeds e2 *)
      simpl in Hcc. inversion Hcc; subst cs.
      destruct (feed_inp_chan_concat _ _ _ _ Hf H) as [ci_s Hci].
      destruct (feed_as_set_inp _ _ _ _ H Hci) as [e2q [Hfq Hsq]].
      rewrite Hf in Hfq. inversion Hfq; subst e2q.
      destruct (IHtransform e1s ci_s (P_Step _ _ _ _ _ Hs1 Hci)) as [e1's [Hss Hr]].
      destruct (lift_seq_L _ _ _ _ Hss e2 c_outer H) as [e2' [Hs2' Hss2]].
      rewrite Hsq in Hs2'. inversion Hs2'; subst e2'.
      exists (ESeq e1's e2p). split; [exact Hss2|].
      eapply T_Seq_L; [eapply set_inp_inp; exact Hsq | exact Hr].
    + (* e2 steps: same on both sides *)
      exists (ESeq e1' e2s). split.
      * eapply PS_step; [|apply PS_refl]. eapply P_Step; [apply S_Seq_R; exact Hs2 | exact Hcc].
      * destruct (step_inp_chan _ _ _ Hs2 _ H) as [ci' [Hinp' Hor]].
        eapply T_Seq_L; [exact Hinp'|].
        destruct Hor as [-> | (π & b & b' & t & o & Hg & Hset)]; [exact H0|].
        eapply fwd_ctx; eauto.
  - (* T_Seq_R *)
    apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
    apply step_ESeq_form in Hs.
    destruct Hs as [(e1s & em1 & e2p & Hs1 & Hf & -> & ->) | (e2s & em2 & Hs2 & -> & ->)].
    + simpl in Hcc. inversion Hcc; subst cs.
      destruct (fwd_feed _ _ _ _ _ H Hf) as [e2'p [Hf' Hr]].
      exists (ESeq e1s e2'p). split; [|apply T_Seq_R; exact Hr].
      eapply PS_step; [|apply PS_refl]. eapply P_Step; [eapply S_Seq_L; [exact Hs1 | exact Hf'] | reflexivity].
    + destruct (IHtransform e2s cs (P_Step _ _ _ _ _ Hs2 Hcc)) as [e2's [Hss Hr]].
      exists (ESeq e1 e2's). split; [apply lift_seq_R; exact Hss | apply T_Seq_R; exact Hr].
  - destruct (Bf_step _ _ _ _ _ H Hstep) as [e's [Hss HB]].
    exists e's. split; [exact Hss | apply T_Base; exact HB].
Qed.

Lemma fwd_sim_star : forall x y, prog_star x y ->
  forall e c ef cf e', x = (e, c) -> y = (ef, cf) -> transform Bf e e' c ->
  exists e'f, prog_star (e', c) (e'f, cf) /\ transform Bf ef e'f cf.
Proof.
  intros x y H. induction H as [x | x y z Hxy Hyz IH]; intros e c ef cf e' Hx Hy Hr; subst.
  - inversion Hy; subst. exists e'. split; [apply PS_refl | exact Hr].
  - destruct y as [ye yc].
    destruct (fwd_sim _ _ _ _ _ Hr Hxy) as [e'y [Hss1 Hr1]].
    destruct (IH ye yc ef cf e'y eq_refl eq_refl Hr1) as [e'f [Hss2 Hr2]].
    exists e'f. split; [eapply prog_star_trans; eauto | exact Hr2].
Qed.

Lemma fwd_done : forall e e' c, transform Bf e e' c -> done e -> done e'.
Proof.
  intros e e' c H. induction H; intros Hd; inversion Hd; subst; try constructor; eauto.
Qed.

End Forward.

(* Typing is preserved along runs. *)
Lemma Type_preservation_star : forall x y, prog_star x y ->
  forall e c e2 c2, x = (e, c) -> y = (e2, c2) -> wt_out e c -> wt_out e2 c2.
Proof.
  intros x y H. induction H as [x | x y z Hxy Hyz IH]; intros e c e2 c2 Hx Hy Hwt.
  - subst. inversion Hy; subst. exact Hwt.
  - subst x. destruct y as [ye yc]. eapply IH; [reflexivity | exact Hy | eapply Type_preservation; eauto].
Qed.

(* ---------------- Reverse simulation ---------------- *)

(* e and e' agree except for the contents of their input buffers. *)
Inductive same_body : Expr -> Expr -> Prop :=
  | SB_Par : forall e1 e2 e1' e2',
      same_body e1 e1' -> same_body e2 e2' -> same_body (EPar e1 e2) (EPar e1' e2')
  | SB_Seq : forall e1 e1' e2,
      same_body e1 e1' -> same_body (ESeq e1 e2) (ESeq e1' e2)
  | SB_Op : forall c c' o l, same_body (EOp c o l) (EOp c' o l).

Lemma same_body_refl : forall e, same_body e e.
Proof. induction e; constructor; auto. Qed.

Lemma same_body_set_inp :
  forall e e' x y ex ey, same_body e e' -> set_inp e x = Some ex -> set_inp e' y = Some ey -> same_body ex ey.
Proof.
  intros e e' x y ex ey H. revert x y ex ey.
  induction H; intros x y ex ey Hx Hy; simpl in Hx, Hy.
  - destruct x as [|x1 x2|? ? ?]; try discriminate. destruct y as [|y1 y2|? ? ?]; try discriminate.
    destruct (set_inp e1 x1) eqn:A1; try discriminate. destruct (set_inp e2 x2) eqn:A2; try discriminate.
    destruct (set_inp e1' y1) eqn:B1; try discriminate. destruct (set_inp e2' y2) eqn:B2; try discriminate.
    inversion Hx; inversion Hy; subst. constructor; eauto.
  - destruct (set_inp e1 x) eqn:A1; try discriminate. destruct (set_inp e1' y) eqn:B1; try discriminate.
    inversion Hx; inversion Hy; subst. constructor; eauto.
  - inversion Hx; inversion Hy; subst. constructor.
Qed.

(* A step of e' is a step of e, if e can read what e' reads. *)
Lemma same_body_step :
  forall e' e's em, step e' e's em ->
  forall e ciL ciR, same_body e e' -> inp_chan e = Some ciL -> inp_chan e' = Some ciR ->
    (exists es, step e es em /\ same_body es e's /\ inp_chan es = Some ciL /\ inp_chan e's = Some ciR) \/
    (exists π v t o ciR', chan_remove ciR π v t o ciR' /\ inp_chan e's = Some ciR' /\
       forall ciL', chan_remove ciL π v t o ciL' ->
         exists es, step e es em /\ same_body es e's /\ inp_chan es = Some ciL').
Proof.
  intros e' e's em H.
  induction H as [e1 e2 e1' c1' Hs IH | e1 e2 e2' c2' Hs IH | e1 e2 e1' c e2' Hs IH Hf
                 | e1 e2 e2' c Hs IH | c o l π v t ord b1 b2 c' o' β Hg Ho Ht Hset Hap];
    intros e ciL ciR Hsb HinL HinR.
  - (* S_Par_L *)
    inversion Hsb as [x1 x2 y1 y2 Hb1 Hb2 | |]; subst. simpl in HinL, HinR.
    destruct (inp_chan x1) as [a|] eqn:EL1; try discriminate. destruct (inp_chan x2) as [b|] eqn:EL2; try discriminate.
    destruct (inp_chan e1) as [a'|] eqn:ER1; try discriminate. destruct (inp_chan e2) as [b'|] eqn:ER2; try discriminate.
    inversion HinL; inversion HinR; subst.
    destruct (IH _ _ _ Hb1 EL1 eq_refl) as [(es & Hs' & Hb & Hi & Hi') | (π & v & t & o & ciR' & Hrm & Hi' & Hall)].
    + left. exists (EPar es x2). split; [apply S_Par_L; exact Hs'|]. split; [constructor; assumption|].
      simpl. rewrite Hi, EL2, Hi', ER2. split; reflexivity.
    + right. exists (DL :: π), v, t, o, (CPair ciR' b').
      split; [apply chan_remove_into_l; exact Hrm|]. split; [simpl; rewrite Hi', ER2; reflexivity|].
      intros ciL' HrmL. destruct (chan_remove_pair_l _ _ _ _ _ _ _ HrmL) as [a'' [Ha'' ->]].
      destruct (Hall _ Ha'') as (es & Hs' & Hb & Hi).
      exists (EPar es x2). split; [apply S_Par_L; exact Hs'|]. split; [constructor; assumption|].
      simpl. rewrite Hi, EL2. reflexivity.
  - (* S_Par_R *)
    inversion Hsb as [x1 x2 y1 y2 Hb1 Hb2 | |]; subst. simpl in HinL, HinR.
    destruct (inp_chan x1) as [a|] eqn:EL1; try discriminate. destruct (inp_chan x2) as [b|] eqn:EL2; try discriminate.
    destruct (inp_chan e1) as [a'|] eqn:ER1; try discriminate. destruct (inp_chan e2) as [b'|] eqn:ER2; try discriminate.
    inversion HinL; inversion HinR; subst.
    destruct (IH _ _ _ Hb2 EL2 eq_refl) as [(es & Hs' & Hb & Hi & Hi') | (π & v & t & o & ciR' & Hrm & Hi' & Hall)].
    + left. exists (EPar x1 es). split; [apply S_Par_R; exact Hs'|]. split; [constructor; assumption|].
      simpl. rewrite Hi, EL1, Hi', ER1. split; reflexivity.
    + right. exists (DR :: π), v, t, o, (CPair a' ciR').
      split; [apply chan_remove_into_r; exact Hrm|]. split; [simpl; rewrite Hi', ER1; reflexivity|].
      intros ciL' HrmL. destruct (chan_remove_pair_r _ _ _ _ _ _ _ HrmL) as [b'' [Hb'' ->]].
      destruct (Hall _ Hb'') as (es & Hs' & Hb & Hi).
      exists (EPar x1 es). split; [apply S_Par_R; exact Hs'|]. split; [constructor; assumption|].
      simpl. rewrite Hi, EL1. reflexivity.
  - (* S_Seq_L: the first part reads; its emission feeds the shared second part *)
    inversion Hsb as [| x1 y1 z2 Hb1 |]; subst. simpl in HinL, HinR.
    destruct (IH _ _ _ Hb1 HinL HinR) as [(es & Hs' & Hb & Hi & Hi') | (π & v & t & o & ciR' & Hrm & Hi' & Hall)].
    + left. exists (ESeq es e2'). split; [eapply S_Seq_L; eauto|]. split; [constructor; assumption|].
      simpl. split; assumption.
    + right. exists π, v, t, o, ciR'. split; [exact Hrm|]. split; [simpl; exact Hi'|].
      intros ciL' HrmL. destruct (Hall _ HrmL) as (es & Hs' & Hb & Hi).
      exists (ESeq es e2'). split; [eapply S_Seq_L; eauto|]. split; [constructor; assumption | simpl; exact Hi].
  - (* S_Seq_R: the shared second part steps *)
    inversion Hsb as [| x1 y1 z2 Hb1 |]; subst. simpl in HinL, HinR.
    left. exists (ESeq x1 e2'). split; [apply S_Seq_R; exact Hs|]. split; [constructor; assumption|].
    simpl. split; assumption.
  - (* S_Op *)
    inversion Hsb as [| | cx cy ox lx]; subst. simpl in HinL, HinR.
    inversion HinL; inversion HinR; subst.
    right. exists π, v, t, ord, c'. split; [exists b1, b2; auto|]. split; [reflexivity|].
    intros ciL' (A & B & Hg' & Ho' & Hs').
    exists (EOp ciL' o' l). split; [eapply S_Op; eauto | split; [constructor | reflexivity]].
Qed.

Lemma same_body_done :
  forall e e' ci ci', same_body e e' -> done e' -> inp_chan e = Some ci -> inp_chan e' = Some ci' ->
    chan_equiv ci ci' -> done e.
Proof.
  intros e e' ci ci' H. revert ci ci'.
  induction H; intros ci ci' Hd Hi Hi' Heq; inversion Hd; subst; simpl in Hi, Hi'.
  - destruct (inp_chan e1) as [a|] eqn:E1; try discriminate. destruct (inp_chan e2) as [b|] eqn:E2; try discriminate.
    destruct (inp_chan e1') as [a'|] eqn:E1'; try discriminate. destruct (inp_chan e2') as [b'|] eqn:E2'; try discriminate.
    inversion Hi; inversion Hi'; subst. inversion Heq; subst.
    constructor; [eapply IHsame_body1 | eapply IHsame_body2]; eauto.
  - constructor; [eapply IHsame_body; eauto | assumption].
  - inversion Hi; inversion Hi'; subst. constructor. eapply chan_equiv_empty; eauto.
Qed.

(* The reverse simulation relation: like transform, but the original side's
   output context cL may run ahead of the rewritten side's cR. *)
Inductive lag_rel (Br : Expr -> Expr -> Chan -> Chan -> Prop) : Expr -> Expr -> Chan -> Chan -> Prop :=
  | L_Base  : forall e e' cL cR, Br e e' cL cR -> lag_rel Br e e' cL cR
  | L_Par_L : forall e1 e1' e2 c1L c1R c2,
      lag_rel Br e1 e1' c1L c1R -> lag_rel Br (EPar e1 e2) (EPar e1' e2) (CPair c1L c2) (CPair c1R c2)
  | L_Par_R : forall e1 e2 e2' c1 c2L c2R,
      lag_rel Br e2 e2' c2L c2R -> lag_rel Br (EPar e1 e2) (EPar e1 e2') (CPair c1 c2L) (CPair c1 c2R)
  | L_Seq_L : forall e1 e1' e2 e2' c ciL ciR,
      inp_chan e2 = Some ciL -> inp_chan e2' = Some ciR -> same_body e2 e2' ->
      lag_rel Br e1 e1' ciL ciR -> lag_rel Br (ESeq e1 e2) (ESeq e1' e2') c c
  | L_Seq_R : forall e1 e2 e2' cL cR,
      lag_rel Br e2 e2' cL cR -> lag_rel Br (ESeq e1 e2) (ESeq e1 e2') cL cR.

Section Reverse.

Variable Br : Expr -> Expr -> Chan -> Chan -> Prop.

Hypothesis Br_step : forall e e' cL cR e's cR',
  Br e e' cL cR -> wt_out e cL -> prog_step (e', cR) (e's, cR') ->
  exists es cL', prog_star (e, cL) (es, cL') /\ Br es e's cL' cR'.
Hypothesis Br_feed : forall e e' cL cR f ep' τ ci,
  Br e e' cL cR -> chan_at_loc f τ -> inp_chan e = Some ci -> chan_sub_type ci τ ->
  feed f e' = Some ep' ->
  exists ep, feed f e = Some ep /\ Br ep ep' cL cR.
Hypothesis Br_consume : forall e e' cL cR π v t o cR',
  Br e e' cL cR -> chan_remove cR π v t o cR' ->
  exists cL', chan_remove cL π v t o cL' /\ Br e e' cL' cR'.
Hypothesis Br_done : forall e e' cL cR, Br e e' cL cR -> done e' -> done e /\ chan_equiv cL cR.

Lemma lag_consume : forall e e' cL cR π v t o cR',
  lag_rel Br e e' cL cR -> chan_remove cR π v t o cR' ->
  exists cL', chan_remove cL π v t o cL' /\ lag_rel Br e e' cL' cR'.
Proof.
  intros e e' cL cR π v t o cR' H. revert π cR'.
  induction H; intros π cR' Hrm.
  - destruct (Br_consume _ _ _ _ _ _ _ _ _ H Hrm) as [cL' [HrmL HB]].
    exists cL'. split; [exact HrmL | apply L_Base; exact HB].
  - destruct π as [|[|] π].
    + destruct Hrm as (A & B & Hg & _); discriminate.
    + destruct (chan_remove_pair_l _ _ _ _ _ _ _ Hrm) as [c1R' [Hr1 ->]].
      destruct (IHlag_rel _ _ Hr1) as [c1L' [HrL Hr]].
      exists (CPair c1L' c2). split; [apply chan_remove_into_l; exact HrL | apply L_Par_L; exact Hr].
    + destruct (chan_remove_pair_r _ _ _ _ _ _ _ Hrm) as [c2' [Hr2 ->]].
      exists (CPair c1L c2'). split; [apply chan_remove_into_r; exact Hr2 | apply L_Par_L; exact H].
  - destruct π as [|[|] π].
    + destruct Hrm as (A & B & Hg & _); discriminate.
    + destruct (chan_remove_pair_l _ _ _ _ _ _ _ Hrm) as [c1' [Hr1 ->]].
      exists (CPair c1' c2L). split; [apply chan_remove_into_l; exact Hr1 | apply L_Par_R; exact H].
    + destruct (chan_remove_pair_r _ _ _ _ _ _ _ Hrm) as [c2R' [Hr2 ->]].
      destruct (IHlag_rel _ _ Hr2) as [c2L' [HrL Hr]].
      exists (CPair c1 c2L'). split; [apply chan_remove_into_r; exact HrL | apply L_Par_R; exact Hr].
  - exists cR'. split; [exact Hrm | eapply L_Seq_L; eauto].
  - destruct (IHlag_rel _ _ Hrm) as [cL' [HrL Hr]].
    exists cL'. split; [exact HrL | apply L_Seq_R; exact Hr].
Qed.

Lemma lag_feed : forall e e' cL cR f ep' τ ci,
  lag_rel Br e e' cL cR -> chan_at_loc f τ -> inp_chan e = Some ci -> chan_sub_type ci τ ->
  feed f e' = Some ep' ->
  exists ep, feed f e = Some ep /\ lag_rel Br ep ep' cL cR.
Proof.
  intros e e' cL cR f ep' τ ci H. revert f ep' τ ci.
  induction H; intros f ep' τ ci Hcal Hinp Hsub Hf.
  - destruct (Br_feed _ _ _ _ _ _ _ _ H Hcal Hinp Hsub Hf) as [ep [Hfe HB]].
    exists ep. split; [exact Hfe | apply L_Base; exact HB].
  - simpl in Hf, Hinp. destruct f as [|fa fb|? ? ?]; try discriminate.
    + inversion Hf; subst. exists (EPar e1 e2). split; [reflexivity | apply L_Par_L; exact H].
    + destruct (feed fa e1') as [a'|] eqn:Ea; try discriminate.
      destruct (feed fb e2) as [b|] eqn:Eb; try discriminate.
      inversion Hf; subst.
      destruct (inp_chan e1) as [ci1|] eqn:Ei1; try discriminate.
      destruct (inp_chan e2) as [ci2|] eqn:Ei2; try discriminate.
      inversion Hinp; subst ci.
      inversion Hcal as [| | ? ? a0 b0 Hca Hcb]; subst.
      inversion Hsub as [| ? ? ? ? Hsa Hsb]; subst.
      destruct (IHlag_rel _ _ _ _ Hca eq_refl Hsa Ea) as [a [Ha Hr]].
      exists (EPar a b). simpl. rewrite Ha, Eb. split; [reflexivity | apply L_Par_L; exact Hr].
  - simpl in Hf, Hinp. destruct f as [|fa fb|? ? ?]; try discriminate.
    + inversion Hf; subst. exists (EPar e1 e2). split; [reflexivity | apply L_Par_R; exact H].
    + destruct (feed fa e1) as [a|] eqn:Ea; try discriminate.
      destruct (feed fb e2') as [b'|] eqn:Eb; try discriminate.
      inversion Hf; subst.
      destruct (inp_chan e1) as [ci1|] eqn:Ei1; try discriminate.
      destruct (inp_chan e2) as [ci2|] eqn:Ei2; try discriminate.
      inversion Hinp; subst ci.
      inversion Hcal as [| | ? ? a0 b0 Hca Hcb]; subst.
      inversion Hsub as [| ? ? ? ? Hsa Hsb]; subst.
      destruct (IHlag_rel _ _ _ _ Hcb eq_refl Hsb Eb) as [b [Hb Hr]].
      exists (EPar a b). simpl. rewrite Ea, Hb. split; [reflexivity | apply L_Par_R; exact Hr].
  - simpl in Hf, Hinp. destruct (feed f e1') as [a'|] eqn:Ea; try discriminate.
    inversion Hf; subst.
    destruct (IHlag_rel _ _ _ _ Hcal Hinp Hsub Ea) as [a [Ha Hr]].
    exists (ESeq a e2). simpl. rewrite Ha. split; [reflexivity | eapply L_Seq_L; eauto].
  - simpl in Hf. destruct (feed f e1) as [a|] eqn:Ea; try discriminate.
    inversion Hf; subst.
    exists (ESeq a e2). simpl. rewrite Ea. split; [reflexivity | apply L_Seq_R; exact H].
Qed.

Lemma lag_sim : forall e e' cL cR e's cR',
  lag_rel Br e e' cL cR -> wt_out e cL -> prog_step (e', cR) (e's, cR') ->
  exists es cL', prog_star (e, cL) (es, cL') /\ lag_rel Br es e's cL' cR'.
Proof.
  intros e e' cL cR e's cR' H. revert e's cR'.
  induction H; intros e's cR' Hwt Hstep.
  - destruct (Br_step _ _ _ _ _ _ H Hwt Hstep) as [es [cL' [Hss HB]]].
    exists es, cL'. split; [exact Hss | apply L_Base; exact HB].
  - (* L_Par_L *)
    apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
    apply step_EPar_form in Hs.
    destruct Hs as [(e1s & em1 & Hs1 & -> & ->) | (e2s & em2 & Hs2 & -> & ->)]; simpl in Hcc.
    + destruct (chan_concat em1 c1R) as [c1R'|] eqn:E1; try discriminate.
      inversion Hcc; subst cR'.
      destruct (IHlag_rel _ _ (wt_out_Par_L_inv _ _ _ _ Hwt) (P_Step _ _ _ _ _ Hs1 E1)) as [e1L [c1L' [Hss Hr]]].
      exists (EPar e1L e2), (CPair c1L' c2). split; [apply lift_par_L; exact Hss | apply L_Par_L; exact Hr].
    + destruct (chan_concat em2 c2) as [c2'|] eqn:E2; try discriminate.
      inversion Hcc; subst cR'.
      exists (EPar e1 e2s), (CPair c1L c2'). split; [|apply L_Par_L; exact H].
      eapply PS_step; [|apply PS_refl]. eapply P_Step; [apply S_Par_R; exact Hs2 | simpl; rewrite E2; reflexivity].
  - (* L_Par_R *)
    apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
    apply step_EPar_form in Hs.
    destruct Hs as [(e1s & em1 & Hs1 & -> & ->) | (e2s & em2 & Hs2 & -> & ->)]; simpl in Hcc.
    + destruct (chan_concat em1 c1) as [c1'|] eqn:E1; try discriminate.
      inversion Hcc; subst cR'.
      exists (EPar e1s e2), (CPair c1' c2L). split; [|apply L_Par_R; exact H].
      eapply PS_step; [|apply PS_refl]. eapply P_Step; [apply S_Par_L; exact Hs1 | simpl; rewrite E1; reflexivity].
    + destruct (chan_concat em2 c2R) as [c2R'|] eqn:E2; try discriminate.
      inversion Hcc; subst cR'.
      destruct (IHlag_rel _ _ (wt_out_Par_R_inv _ _ _ _ Hwt) (P_Step _ _ _ _ _ Hs2 E2)) as [e2L [c2L' [Hss Hr]]].
      exists (EPar e1 e2L), (CPair c1 c2L'). split; [apply lift_par_R; exact Hss | apply L_Par_R; exact Hr].
  - (* L_Seq_L *)
    apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
    apply step_ESeq_form in Hs.
    destruct Hs as [(e1s & em1 & e2p & Hs1 & Hf & -> & ->) | (e2s & em2 & Hs2 & -> & ->)].
    + (* the rewritten first part steps and feeds e2' *)
      simpl in Hcc. inversion Hcc; subst cR'.
      destruct (feed_inp_chan_concat _ _ _ _ Hf H0) as [ciR' Hci].
      destruct (feed_as_set_inp _ _ _ _ H0 Hci) as [e2q [Hfq Hsq]].
      rewrite Hf in Hfq. inversion Hfq; subst e2q.
      destruct (IHlag_rel _ _ (wt_out_Seq_L_inv _ _ _ _ Hwt H) (P_Step _ _ _ _ _ Hs1 Hci)) as [e1L [ciL' [Hss Hr]]].
      destruct (lift_seq_L _ _ _ _ Hss e2 c H) as [e2L [Hs2L Hss2]].
      exists (ESeq e1L e2L), c. split; [exact Hss2|].
      eapply L_Seq_L; [eapply set_inp_inp; exact Hs2L | eapply set_inp_inp; exact Hsq
                      | eapply same_body_set_inp; eauto | exact Hr].
    + (* the shared second part steps *)
      destruct (same_body_step _ _ _ Hs2 _ _ _ H1 H H0)
        as [(es & Hs & Hb & Hi & Hi') | (π & v & t & o & ciR' & Hrm & Hi' & Hall)].
      * exists (ESeq e1 es), cR'. split; [|eapply L_Seq_L; eauto].
        eapply PS_step; [|apply PS_refl]. eapply P_Step; [apply S_Seq_R; exact Hs | exact Hcc].
      * destruct (lag_consume _ _ _ _ _ _ _ _ _ H2 Hrm) as [ciL' [HrmL Hr]].
        destruct (Hall _ HrmL) as (es & Hs & Hb & Hi).
        exists (ESeq e1 es), cR'. split; [|eapply L_Seq_L; eauto].
        eapply PS_step; [|apply PS_refl]. eapply P_Step; [apply S_Seq_R; exact Hs | exact Hcc].
  - (* L_Seq_R *)
    apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
    apply step_ESeq_form in Hs.
    destruct Hs as [(e1s & em1 & e2p & Hs1 & Hf & -> & ->) | (e2s & em2 & Hs2 & -> & ->)].
    + simpl in Hcc. inversion Hcc; subst cR'.
      destruct (wt_out_ESeq_inv _ _ _ Hwt) as (τ1 & τ2 & c_in & Hw1 & Hinp & Hsub & Hw2 & Hco).
      destruct (lag_feed _ _ _ _ _ _ _ _ H (step_at_loc _ _ _ _ Hs1 Hw1) Hinp Hsub Hf) as [e2L [HfL Hr]].
      exists (ESeq e1s e2L), cL. split; [|apply L_Seq_R; exact Hr].
      eapply PS_step; [|apply PS_refl]. eapply P_Step; [eapply S_Seq_L; [exact Hs1 | exact HfL] | reflexivity].
    + destruct (IHlag_rel _ _ (wt_out_Seq_R_inv _ _ _ Hwt) (P_Step _ _ _ _ _ Hs2 Hcc)) as [e2L [cL' [Hss Hr]]].
      exists (ESeq e1 e2L), cL'. split; [apply lift_seq_R; exact Hss | apply L_Seq_R; exact Hr].
Qed.

Lemma lag_sim_star : forall x y, prog_star x y ->
  forall e' cR e'f cRf e cL, x = (e', cR) -> y = (e'f, cRf) -> lag_rel Br e e' cL cR -> wt_out e cL ->
  exists ef cLf, prog_star (e, cL) (ef, cLf) /\ lag_rel Br ef e'f cLf cRf.
Proof.
  intros x y H. induction H as [x | x y z Hxy Hyz IH]; intros e' cR e'f cRf e cL Hx Hy Hr Hwt; subst.
  - inversion Hy; subst. exists e, cL. split; [apply PS_refl | exact Hr].
  - destruct y as [ye yc].
    destruct (lag_sim _ _ _ _ _ _ Hr Hwt Hxy) as [ey [cLy [Hss1 Hr1]]].
    destruct (IH ye yc e'f cRf ey cLy eq_refl eq_refl Hr1 (Type_preservation_star _ _ Hss1 _ _ _ _ eq_refl eq_refl Hwt))
      as [ef [cLf [Hss2 Hr2]]].
    exists ef, cLf. split; [eapply prog_star_trans; eauto | exact Hr2].
Qed.

Lemma lag_done : forall e e' cL cR, lag_rel Br e e' cL cR -> done e' -> done e /\ chan_equiv cL cR.
Proof.
  intros e e' cL cR H. induction H; intros Hd.
  - eapply Br_done; eassumption.
  - inversion Hd as [x1 x2 Hd1 Hd2 | |]; subst. destruct (IHlag_rel Hd1) as [Hd1' Heq].
    split; [constructor; assumption | constructor; [exact Heq | apply chan_equiv_refl]].
  - inversion Hd as [x1 x2 Hd1 Hd2 | |]; subst. destruct (IHlag_rel Hd2) as [Hd2' Heq].
    split; [constructor; assumption | constructor; [apply chan_equiv_refl | exact Heq]].
  - inversion Hd as [| x1 x2 Hd1 Hd2 |]; subst. destruct (IHlag_rel Hd1) as [Hd1' Heq].
    split; [constructor; [exact Hd1' | eapply same_body_done; eauto] | apply chan_equiv_refl].
  - inversion Hd as [| x1 x2 Hd1 Hd2 |]; subst. destruct (IHlag_rel Hd2) as [Hd2' Heq].
    split; [constructor; assumption | exact Heq].
Qed.

End Reverse.

Lemma transform_to_lag :
  forall (B : Expr -> Expr -> Chan -> Prop) (Br : Expr -> Expr -> Chan -> Chan -> Prop),
    (forall e e' c, B e e' c -> Br e e' c c) ->
    forall e e' c, transform B e e' c -> lag_rel Br e e' c c.
Proof.
  intros B Br HB e e' c H. induction H.
  - apply L_Par_L. exact IHtransform.
  - apply L_Par_R. exact IHtransform.
  - eapply L_Seq_L; [exact H | exact H | apply same_body_refl | exact IHtransform].
  - apply L_Seq_R. exact IHtransform.
  - apply L_Base. auto.
Qed.

(* ---------------- Soundness from the two simulations ---------------- *)

(* The conditions a base rewrite's forward relation Bf must satisfy. *)
Definition fwd_conds (Bf : Expr -> Expr -> Chan -> Prop) : Prop :=
  (forall e e' c es cs, Bf e e' c -> prog_step (e, c) (es, cs) ->
     exists e's, prog_star (e', c) (e's, cs) /\ Bf es e's cs) /\
  (forall e e' c f ep, Bf e e' c -> feed f e = Some ep ->
     exists ep', feed f e' = Some ep' /\ Bf ep ep' c) /\
  (forall e e' c π b b' t o c', Bf e e' c -> chan_get c π = Some (CBuf b t o) ->
     chan_set c π (CBuf b' t o) = Some c' -> Bf e e' c') /\
  (forall e e' c, Bf e e' c -> done e -> done e').

(* The conditions a base rewrite's reverse relation Br must satisfy. *)
Definition rev_conds (Br : Expr -> Expr -> Chan -> Chan -> Prop) : Prop :=
  (forall e e' cL cR e's cR', Br e e' cL cR -> wt_out e cL -> prog_step (e', cR) (e's, cR') ->
     exists es cL', prog_star (e, cL) (es, cL') /\ Br es e's cL' cR') /\
  (forall e e' cL cR f ep' τ ci, Br e e' cL cR -> chan_at_loc f τ -> inp_chan e = Some ci ->
     chan_sub_type ci τ -> feed f e' = Some ep' ->
     exists ep, feed f e = Some ep /\ Br ep ep' cL cR) /\
  (forall e e' cL cR π v t o cR', Br e e' cL cR -> chan_remove cR π v t o cR' ->
     exists cL', chan_remove cL π v t o cL' /\ Br e e' cL' cR') /\
  (forall e e' cL cR, Br e e' cL cR -> done e' -> done e /\ chan_equiv cL cR).

Lemma fwd_conds_union : forall Bf1 Bf2, fwd_conds Bf1 -> fwd_conds Bf2 ->
  fwd_conds (fun e e' c => Bf1 e e' c \/ Bf2 e e' c).
Proof.
  intros Bf1 Bf2 (S1 & F1 & C1 & D1) (S2 & F2 & C2 & D2). refine (conj _ (conj _ (conj _ _))).
  - intros e e' c es cs [H|H] Hs.
    + destruct (S1 _ _ _ _ _ H Hs) as [e's [Hss Hr]]. exists e's. auto.
    + destruct (S2 _ _ _ _ _ H Hs) as [e's [Hss Hr]]. exists e's. auto.
  - intros e e' c f ep [H|H] Hf.
    + destruct (F1 _ _ _ _ _ H Hf) as [ep' [Hf' Hr]]. exists ep'. auto.
    + destruct (F2 _ _ _ _ _ H Hf) as [ep' [Hf' Hr]]. exists ep'. auto.
  - intros e e' c π b b' t o c' [H|H] Hg Hs; [left; eapply C1 | right; eapply C2]; eauto.
  - intros e e' c [H|H] Hd; [eapply D1 | eapply D2]; eauto.
Qed.

Lemma rev_conds_union : forall Br1 Br2, rev_conds Br1 -> rev_conds Br2 ->
  rev_conds (fun e e' cL cR => Br1 e e' cL cR \/ Br2 e e' cL cR).
Proof.
  intros Br1 Br2 (S1 & F1 & C1 & D1) (S2 & F2 & C2 & D2). refine (conj _ (conj _ (conj _ _))).
  - intros e e' cL cR e's cR' [H|H] Hwt Hs.
    + destruct (S1 _ _ _ _ _ _ H Hwt Hs) as [es [cL' [Hss Hr]]]. exists es, cL'. auto.
    + destruct (S2 _ _ _ _ _ _ H Hwt Hs) as [es [cL' [Hss Hr]]]. exists es, cL'. auto.
  - intros e e' cL cR f ep' τ ci [H|H] Hcal Hinp Hsub Hf.
    + destruct (F1 _ _ _ _ _ _ _ _ H Hcal Hinp Hsub Hf) as [ep [Hfe Hr]]. exists ep. auto.
    + destruct (F2 _ _ _ _ _ _ _ _ H Hcal Hinp Hsub Hf) as [ep [Hfe Hr]]. exists ep. auto.
  - intros e e' cL cR π v t o cR' [H|H] Hrm.
    + destruct (C1 _ _ _ _ _ _ _ _ _ H Hrm) as [cL' [Hrm' Hr]]. exists cL'. auto.
    + destruct (C2 _ _ _ _ _ _ _ _ _ H Hrm) as [cL' [Hrm' Hr]]. exists cL'. auto.
  - intros e e' cL cR [H|H] Hd; [eapply D1 | eapply D2]; eauto.
Qed.

(* Final outputs are preserved up to chan_equiv by any base rewrite B
   contained in relations Bf and Br satisfying the conditions. *)
Theorem transform_fin_soundness :
  forall (B Bf : Expr -> Expr -> Chan -> Prop) (Br : Expr -> Expr -> Chan -> Chan -> Prop),
    (forall e e' c, B e e' c -> Bf e e' c) ->
    (forall e e' c, B e e' c -> Br e e' c c) ->
    fwd_conds Bf -> rev_conds Br ->
    forall e e' c,
      transform B e e' c ->
      wt_out e c ->
      forall c_final,
        (fin_output e c c_final -> exists c', fin_output e' c c' /\ chan_equiv c_final c') /\
        (fin_output e' c c_final -> exists c', fin_output e c c' /\ chan_equiv c' c_final).
Proof.
  intros B Bf Br HBf HBr (Fs & Ff & Fc & Fd) (Rs & Rf & Rc & Rd) e e' c Htr Hwt c_final.
  pose proof (transform_mono _ _ HBf _ _ _ Htr) as Hf.
  split.
  - intros [ef [Hss Hd]].
    destruct (fwd_sim_star Bf Fs Ff Fc _ _ Hss _ _ _ _ _ eq_refl eq_refl Hf) as [e'f [Hss' Hr]].
    exists c_final. split; [exists e'f; split; [exact Hss' | eapply fwd_done; eauto] | apply chan_equiv_refl].
  - intros [e'f [Hss Hd]].
    destruct (lag_sim_star Br Rs Rf Rc _ _ Hss _ _ _ _ _ _ eq_refl eq_refl
                (transform_to_lag _ _ HBr _ _ _ Htr) Hwt) as [ef [cLf [Hss' Hr]]].
    destruct (lag_done Br Rd _ _ _ _ Hr Hd) as [Hdf Heq].
    exists cLf. split; [exists ef; split; assumption | exact Heq].
Qed.

(* ============================================================================
   17. The relabeling rules satisfy the simulation conditions, in lockstep:
       each step of one side is matched by exactly one step of the other,
       and both sides always share their output context.
   ========================================================================== *)

(* Relabeling one buffer keeps every buffer's contents. *)
Lemma relabel_empty :
  forall c π b t o t' c',
    chan_get c π = Some (CBuf b t o) ->
    chan_set c π (CBuf b t' o) = Some c' ->
    (forall b0 t0 o0, buf_in b0 t0 o0 c -> b0 = []) ->
    (forall b0 t0 o0, buf_in b0 t0 o0 c' -> b0 = []).
Proof.
  intros c π b t o t' c' Hget Hset Hall b0 t0 o0 Hin.
  destruct (chan_set_buf_in _ _ _ _ _ _ _ Hset Hin) as [H|H].
  - exact (Hall _ _ _ H).
  - destruct (buf_in_cbuf _ _ _ _ _ _ H) as (-> & _ & _).
    eapply Hall. exists π. exact Hget.
Qed.

Lemma relabel_empty_rev :
  forall c π b t o t' c',
    chan_get c π = Some (CBuf b t o) ->
    chan_set c π (CBuf b t' o) = Some c' ->
    (forall b0 t0 o0, buf_in b0 t0 o0 c' -> b0 = []) ->
    (forall b0 t0 o0, buf_in b0 t0 o0 c -> b0 = []).
Proof.
  intros c π b t o t' c' Hget Hset.
  eapply relabel_empty; [eapply chan_set_get_inv; exact Hset | eapply chan_set_inverse; eauto].
Qed.

Lemma relabel_done : forall e e' c, relabel e e' c -> done e -> done e'.
Proof.
  intros e e' c_outer0 Hrl Hd. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1]; inversion Hd; subst.
  - constructor. eapply relabel_empty; eauto.
  - constructor. assumption.
  - constructor; [assumption|].
    match goal with Hx : done (EOp _ _ _) |- _ => inversion Hx; subst end.
    constructor. eapply relabel_empty; eauto.
Qed.

Lemma relabel_done_rev : forall e e' c, relabel e e' c -> done e' -> done e.
Proof.
  intros e e' c_outer0 Hrl Hd. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1]; inversion Hd; subst.
  - constructor. eapply relabel_empty_rev; eauto.
  - constructor. assumption.
  - constructor; [assumption|].
    match goal with Hx : done (EOp _ _ _) |- _ => inversion Hx; subst end.
    constructor. eapply relabel_empty_rev; eauto.
Qed.

(* The output context may lose (or gain) buffer contents. *)
Lemma relabel_ctx :
  forall e e' c_outer, relabel e e' c_outer ->
    forall c_new π_set b_old b_new t_set o_set,
      chan_get c_outer π_set = Some (CBuf b_old t_set o_set) ->
      chan_set c_outer π_set (CBuf b_new t_set o_set) = Some c_new ->
      relabel e e' c_new.
Proof.
  intros e e' c_outer0 Hrl. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1];
    intros c_new π_set b_old b_new t_set o_set Hget Hset.
  - (* L2N: c_outer not constrained by rule's premises *)
    eapply R_L2N; eauto.
  - (* NLN: requires all_net c_outer; preserved by same-type set if original type was TNet *)
    assert (Ht_net : t_set = TNet).
    { eapply all_net_chan_get_TNet; [exact H0 | exact Hget]. }
    subst t_set.
    apply R_NLN; [exact H |].
    eapply chan_set_TNet_preserves_all_net; [exact H0 | exact Hset].
  - (* LNL: c_outer not constrained *)
    eapply R_LNL; eauto.
Qed.

(* Input arrives on both sides. *)
Lemma relabel_feed :
  forall e e' c_outer, relabel e e' c_outer ->
    forall c_feed e_post, feed c_feed e = Some e_post ->
    exists e_post', feed c_feed e' = Some e_post' /\ relabel e_post e_post' c_outer.
Proof.
  intros e e' c_outer0 Hrl. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1];
    intros c_feed e_post Hfeed.
  - (* L2N *)
    simpl in Hfeed.
    destruct (chan_concat c_feed c) as [c_LHS_post|] eqn:Ecc; try discriminate.
    inversion Hfeed; subst e_post. clear Hfeed.
    (* Use chan_concat_TLoc_TNet_at_path *)
    destruct (chan_concat_TLoc_TNet_at_path π c_feed c c' b l ord c_LHS_post
                H H0 Ecc) as [c_RHS_post [B [HccR [Hg_post Hs_post]]]].
    exists (EOp c_RHS_post o l). split.
    + simpl. rewrite HccR. reflexivity.
    + eapply R_L2N; [exact Hg_post | exact Hs_post].

  - (* NLN *)
    simpl in Hfeed.
    destruct (chan_concat c_feed c) as [c_post|] eqn:Ecc; try discriminate.
    inversion Hfeed; subst e_post. clear Hfeed.
    exists (EOp c_post o l'). split.
    + simpl. rewrite Ecc. reflexivity.
    + apply R_NLN.
      * eapply chan_concat_all_net_r. exact H. exact Ecc.
      * exact H0.

  - (* LNL *)
    simpl in Hfeed.
    destruct (feed c_feed e1) as [e1_post|] eqn:Ef1; try discriminate.
    inversion Hfeed; subst e_post. clear Hfeed.
    exists (ESeq e1_post (EOp c' o2 l)). split.
    + simpl. rewrite Ef1. reflexivity.
    + destruct (feed_preserves_expr_get_loc e1 c_feed e1_post Ef1 π c1 o1 l H)
        as [c1_new [o1_new Hex_new]].
      eapply R_LNL with (π := π) (b := b); [exact Hex_new | exact H0 | exact H1].
Qed.

(* Arrival on the rewritten side implies arrival on the original side, given
   the original's typing (L2N: the arriving values come from location l). *)
Lemma relabel_feed_rev :
  forall e e' c_outer, relabel e e' c_outer ->
    forall c_feed cin τ e_post_R,
      chan_at_loc c_feed τ ->
      inp_chan e = Some cin ->
      chan_sub_type cin τ ->
      feed c_feed e' = Some e_post_R ->
      exists e_post, feed c_feed e = Some e_post.
Proof.
  intros e e' c_outer0 Hrl. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1];
    intros c_feed cin τ e_post_R Hcal Hinp Hsub Hfeed_R.
  - (* L2N: relabel (EOp c o l) (EOp c' o l) c_outer
       (chan_get c π = Some (CBuf b (TLoc l)), chan_set c π (CBuf b TNet) = Some c') *)
    simpl in Hfeed_R.
    destruct (chan_concat c_feed c') as [r_RHS|] eqn:Ecc_R; try discriminate.
    inversion Hfeed_R; subst e_post_R. clear Hfeed_R.
    simpl in Hinp. inversion Hinp; subst cin. clear Hinp.
    (* Hsub: chan_sub_type c τ.  Hcal: chan_at_loc c_feed τ.
       Use chan_at_loc_subtype_concat (which works for the LHS c). *)
    destruct (chan_at_loc_subtype_concat c_feed c τ Hcal Hsub) as [r_LHS Ecc_L].
    exists (EOp r_LHS o l). simpl. rewrite Ecc_L. reflexivity.

  - (* NLN: relabel (EOp c o l) (EOp c o l') c_outer (channel unchanged). *)
    simpl in Hfeed_R.
    destruct (chan_concat c_feed c) as [r|] eqn:Ecc; try discriminate.
    inversion Hfeed_R; subst e_post_R. clear Hfeed_R.
    exists (EOp r o l). simpl. rewrite Ecc. reflexivity.

  - (* LNL: relabel (ESeq e1 (EOp c o2 l)) (ESeq e1 (EOp c' o2 l)) c_outer
       (e1 unchanged; ESeq feeds into e1, not the inner EOp). *)
    simpl in Hfeed_R.
    destruct (feed c_feed e1) as [a_post|] eqn:Efa; try discriminate.
    inversion Hfeed_R; subst e_post_R. clear Hfeed_R.
    exists (ESeq a_post (EOp c o2 l)). simpl. rewrite Efa. reflexivity.
Qed.

(* Reading a buffer of c commutes with relabeling one of its buffers: c' is c
   with the buffer at π relabeled from tA to tB.  A read at π1 of c is a read
   at π1 of c' (with the new label if π1 = π), and the results are again
   related by relabeling the buffer at π. *)
Lemma read_relabeled :
  forall c π b tA tB ord c' π1 X Y t1 o1 d,
    chan_get c π = Some (CBuf b tA ord) ->
    chan_set c π (CBuf b tB ord) = Some c' ->
    chan_get c π1 = Some (CBuf X t1 o1) ->
    chan_set c π1 (CBuf Y t1 o1) = Some d ->
    exists t1' d' B,
      chan_get c' π1 = Some (CBuf X t1' o1) /\
      chan_set c' π1 (CBuf Y t1' o1) = Some d' /\
      ((π1 = π /\ t1 = tA /\ t1' = tB) \/ t1' = t1) /\
      chan_get d π = Some (CBuf B tA ord) /\
      chan_set d π (CBuf B tB ord) = Some d'.
Proof.
  intros c π b tA tB ord c' π1 X Y t1 o1 d Hg Hs Hg1 Hs1.
  destruct (path_eq_dec π1 π) as [-> | Hne].
  - rewrite Hg in Hg1. injection Hg1 as E1 E2 E3. subst X t1 o1.
    pose proof (chan_set_get_inv _ _ _ _ Hs) as Hg'.
    destruct (chan_set_at_buf_exists _ _ _ _ _ (CBuf Y tB ord) Hg') as [d' Hs'].
    exists tB, d', Y. split; [exact Hg'|]. split; [exact Hs'|]. split; [left; auto|].
    split; [eapply chan_set_get_inv; exact Hs1|].
    eapply chan_set_replace; [exact Hs1 | eapply chan_set_set_same; [exact Hs | exact Hs']].
  - pose proof (chan_set_buf_get_buf π π1 c b tA ord (CBuf b tB ord) c' X t1 o1 Hg Hs Hg1) as Hcs.
    destruct Hcs as [[Heq _]|[_ Hg']]; [exfalso; apply Hne; symmetry; exact Heq|].
    destruct (chan_set_at_buf_exists _ _ _ _ _ (CBuf Y t1 o1) Hg') as [d' Hs'].
    pose proof (chan_set_buf_get_buf π1 π c X t1 o1 (CBuf Y t1 o1) d b tA ord Hg1 Hs1 Hg) as Hcs2.
    destruct Hcs2 as [[Heq _]|[_ Hgd]]; [exfalso; apply Hne; exact Heq|].
    exists t1, d', b. split; [exact Hg'|]. split; [exact Hs'|]. split; [right; reflexivity|].
    split; [exact Hgd|].
    eapply chan_set_diverge_commute with (c := c) (π1 := π) (π2 := π1) (c1 := c') (c2 := d);
      [exact Hg | exact Hg1 | intro Heq; apply Hne; symmetry; exact Heq | exact Hs | exact Hs1 | exact Hs'].
Qed.

(* Relabeling is reversible. *)
Lemma relabel_flip : forall c π b tA tB ord c',
  chan_get c π = Some (CBuf b tA ord) -> chan_set c π (CBuf b tB ord) = Some c' ->
  chan_get c' π = Some (CBuf b tB ord) /\ chan_set c' π (CBuf b tA ord) = Some c.
Proof. intros. split; [eapply chan_set_get_inv; eauto | eapply chan_set_inverse; eauto]. Qed.

Lemma relabel_step :
  forall e e' c_outer, relabel e e' c_outer ->
    forall e_step c_step c_inner_LHS,
      step e e_step c_inner_LHS ->
      chan_concat c_inner_LHS c_outer = Some c_step ->
      exists e'_step c_inner_RHS,
        step e' e'_step c_inner_RHS /\
        chan_concat c_inner_RHS c_outer = Some c_step /\
        relabel e_step e'_step c_step.
Proof.
  intros e e' c_outer0 Hrl. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1];
    intros e_step c_step c_inner_LHS Hstep Hcc.
  - (* L2N: a read of the relabeled buffer is allowed on both sides *)
    apply step_EOp_form in Hstep.
    destruct Hstep as (π1 & v & t1 & ord1 & b1 & b2 & c'' & o' & β & Hg1 & Hfifo & Ht1 & Hs1 & Hap & -> & ->).
    destruct (read_relabeled _ _ _ _ _ _ _ _ _ _ _ _ _ H H0 Hg1 Hs1)
      as (t1' & d' & B & Hg' & Hs' & Ht & HgB & HsB).
    exists (EOp d' o' l), (wrap_loc β (TLoc l)).
    split; [|split; [exact Hcc | eapply R_L2N; eauto]].
    eapply S_Op; [exact Hg' | exact Hfifo | | exact Hs' | exact Hap].
    destruct Ht as [(_ & _ & ->) | ->]; [right; reflexivity | exact Ht1].

  - (* NLN: the emission is stamped at l vs l', but lands in TNet buffers *)
    apply step_EOp_form in Hstep.
    destruct Hstep as (π & v & t & ord & b1 & b2 & c' & o' & β & Hg & Hfifo & Ht & Hs & Hap & Heq & Hci).
    subst e_step c_inner_LHS.
    assert (Ht_net : t = TNet) by (eapply all_net_chan_get_TNet; [exact H | exact Hg]).
    subst t.
    exists (EOp c' o' l'), (wrap_loc β (TLoc l')).
    split.
    { eapply S_Op with (π := π) (v := v) (t := TNet) (ord := ord) (b1 := b1) (b2 := b2);
        [exact Hg | exact Hfifo | right; reflexivity | exact Hs | exact Hap]. }
    split. { rewrite (chan_concat_wrap_loc_indep β l' l c_outer H0). exact Hcc. }
    apply R_NLN.
    + eapply chan_set_TNet_preserves_all_net; [exact H | exact Hs].
    + eapply chan_concat_all_net_r; [exact H0 | exact Hcc].

  - (* LNL *)
    apply step_ESeq_form in Hstep.
    destruct Hstep as [[e1_s [c'' [eop_s [Hs1 [Hfeed [Heq1 Heq2]]]]]] |
                       [eop_s [c''' [Hs_eop [Heq1 Heq2]]]]].
    + (* S_Seq_L: step on e1, then feed into inner EOp. Use step_output_loc_at_path
         to show c'' is location-compatible at π, then chan_concat_TNet_TLoc_at_path
         gives the swap that closes LNL on the post-step. *)
      subst e_step c_inner_LHS.
      simpl in Hcc. inversion Hcc; subst c_step. clear Hcc.
      simpl in Hfeed.
      destruct (chan_concat c'' c) as [inner_LHS|] eqn:Ecc; try discriminate.
      inversion Hfeed; subst eop_s. clear Hfeed.
      assert (Hloc_c'' : forall b' t' o',
                          chan_get c'' π = Some (CBuf b' t' o') -> t' = TLoc l).
      { intros b' t' o' Hgc''.
        eapply step_output_loc_at_path; [exact Hs1 | exact H | exact Hgc'']. }
      destruct (chan_concat_TNet_TLoc_at_path π c'' c c' b l ord inner_LHS
                  H0 H1 Ecc Hloc_c'') as
        [inner_RHS [B [Hcc_R [Hg_LHS Hset_LHS_R]]]].
      destruct (step_preserves_expr_get_loc e1 e1_s c'' Hs1 π c1 o1 l H)
        as [c1_s [o1_s Hex_s]].
      exists (ESeq e1_s (EOp inner_RHS o2 l)), CEps.
      split.
      { eapply S_Seq_L; [exact Hs1 |]. simpl. rewrite Hcc_R. reflexivity. }
      split. { simpl. reflexivity. }
      eapply R_LNL with (π := π) (b := B); [exact Hex_s | exact Hg_LHS | exact Hset_LHS_R].
    + (* S_Seq_R: the inner operator reads; a read of the relabeled buffer is
         allowed on both sides *)
      subst e_step c_inner_LHS.
      apply step_EOp_form in Hs_eop.
      destruct Hs_eop as (π1 & v & t1 & ord1 & b1 & b2 & c_post & o' & β & Hg1 & Hfifo & Ht1 & Hs1' & Hap & -> & ->).
      destruct (read_relabeled _ _ _ _ _ _ _ _ _ _ _ _ _ H0 H1 Hg1 Hs1')
        as (t1' & d' & B & Hg' & Hs' & Ht & HgB & HsB).
      exists (ESeq e1 (EOp d' o' l)), (wrap_loc β (TLoc l)).
      split; [|split; [exact Hcc | eapply R_LNL; eauto]].
      eapply S_Seq_R. eapply S_Op; [exact Hg' | exact Hfifo | | exact Hs' | exact Hap].
      destruct Ht as [(_ & _ & ->) | ->]; [left; reflexivity | exact Ht1].
Qed.

Lemma relabel_step_rev :
  forall e e' c_outer, relabel e e' c_outer ->
    forall e'_step c_step c_inner_RHS,
      step e' e'_step c_inner_RHS ->
      chan_concat c_inner_RHS c_outer = Some c_step ->
      exists e_step c_inner_LHS,
        step e e_step c_inner_LHS /\
        chan_concat c_inner_LHS c_outer = Some c_step /\
        relabel e_step e'_step c_step.
Proof.
  intros e e' c_outer0 Hrl. destruct Hrl as [c c' o l π b ord c_outer H H0 | c o l l' c_outer H H0 | e1 π c1 o1 l c c' o2 b ord c_outer H H0 H1];
    intros e'_step c_step c_inner_RHS Hstep Hcc.
  - (* L2N: the rewritten side reads; so can the original *)
    apply step_EOp_form in Hstep.
    destruct Hstep as (π1 & v & t1 & ord1 & b1 & b2 & c'_post & o' & β & Hg1 & Hfifo & Ht1 & Hs1 & Hap & -> & ->).
    destruct (relabel_flip _ _ _ _ _ _ _ H H0) as [HgF HsF].
    destruct (read_relabeled _ _ _ _ _ _ _ _ _ _ _ _ _ HgF HsF Hg1 Hs1)
      as (t1' & d & B & Hg' & Hs' & Ht & HgB & HsB).
    destruct (relabel_flip _ _ _ _ _ _ _ HgB HsB) as [HgB' HsB'].
    exists (EOp d o' l), (wrap_loc β (TLoc l)).
    split; [|split; [exact Hcc | eapply R_L2N; eauto]].
    eapply S_Op; [exact Hg' | exact Hfifo | | exact Hs' | exact Hap].
    destruct Ht as [(_ & _ & ->) | ->]; [left; reflexivity | exact Ht1].

  - (* NLN *)
    apply step_EOp_form in Hstep.
    destruct Hstep as (pi & v & t & ord & b1 & b2 & c' & o' & bb & Hg & Hfifo & Ht & Hs & Hap & Heq & Hci).
    subst e'_step c_inner_RHS.
    assert (Ht_net : t = TNet) by (eapply all_net_chan_get_TNet; [exact H | exact Hg]).
    subst t.
    exists (EOp c' o' l), (wrap_loc bb (TLoc l)).
    split.
    { eapply S_Op with (π := pi) (v := v) (t := TNet) (ord := ord) (b1 := b1) (b2 := b2);
        [exact Hg | exact Hfifo | right; reflexivity | exact Hs | exact Hap]. }
    split. { rewrite (chan_concat_wrap_loc_indep bb l l' c_outer H0). exact Hcc. }
    apply R_NLN.
    + eapply chan_set_TNet_preserves_all_net; [exact H | exact Hs].
    + eapply chan_concat_all_net_r; [exact H0 | exact Hcc].

  - (* LNL *)
    apply step_ESeq_form in Hstep.
    destruct Hstep as [[e1_s [c'' [eop_s [Hs1 [Hfeed [Heq1 Heq2]]]]]] |
                       [eop_s [c''' [Hs_eop [Heq1 Heq2]]]]].
    + subst e'_step c_inner_RHS.
      simpl in Hcc. inversion Hcc; subst c_step. clear Hcc.
      simpl in Hfeed.
      destruct (chan_concat c'' c') as [inner_RHS|] eqn:Ecc; try discriminate.
      inversion Hfeed; subst eop_s. clear Hfeed.
      pose proof (chan_set_get_inv _ _ _ _ H1) as Hg_c'.
      pose proof (chan_set_inverse π c (CBuf b TNet ord) (CBuf b (TLoc l) ord) c' H0 H1) as Hinv.
      destruct (chan_concat_TLoc_TNet_at_path π c'' c' c b l ord inner_RHS
                  Hg_c' Hinv Ecc) as [inner_LHS [B [Hcc_LHS [Hg_RHS Hset_RHS_LHS]]]].
      destruct (step_preserves_expr_get_loc e1 e1_s c'' Hs1 π c1 o1 l H)
        as [c1_s [o1_s Hex_s]].
      exists (ESeq e1_s (EOp inner_LHS o2 l)), CEps.
      split.
      { eapply S_Seq_L; [exact Hs1 |]. simpl. rewrite Hcc_LHS. reflexivity. }
      split. { simpl. reflexivity. }
      eapply R_LNL with (π := π) (b := B) (ord := ord).
      * exact Hex_s.
      * eapply chan_set_get_inv. exact Hset_RHS_LHS.
      * eapply chan_set_replace with (c := inner_RHS) (v1 := CBuf B TNet ord).
        -- exact Hset_RHS_LHS.
        -- destruct (chan_set_at_buf_exists π inner_RHS B (TLoc l) ord (CBuf B (TLoc l) ord) Hg_RHS) as [c_x Hsetx].
           pose proof (chan_set_set_back π inner_RHS (CBuf B (TLoc l) ord) c_x Hg_RHS Hsetx) as Hxeq.
           subst c_x. exact Hsetx.
    + (* S_Seq_R: the rewritten side's inner operator reads; so can the original's *)
      subst e'_step c_inner_RHS.
      apply step_EOp_form in Hs_eop.
      destruct Hs_eop as (π1 & v & t1 & ord1 & b1 & b2 & c_post & o' & β & Hg1 & Hfifo & Ht1 & Hs1' & Hap & -> & ->).
      destruct (relabel_flip _ _ _ _ _ _ _ H0 H1) as [HgF HsF].
      destruct (read_relabeled _ _ _ _ _ _ _ _ _ _ _ _ _ HgF HsF Hg1 Hs1')
        as (t1' & d & B & Hg' & Hs' & Ht & HgB & HsB).
      destruct (relabel_flip _ _ _ _ _ _ _ HgB HsB) as [HgB' HsB'].
      exists (ESeq e1 (EOp d o' l)), (wrap_loc β (TLoc l)).
      split; [|split; [exact Hcc | eapply R_LNL; eauto]].
      eapply S_Seq_R. eapply S_Op; [exact Hg' | exact Hfifo | | exact Hs' | exact Hap].
      destruct Ht as [(_ & _ & ->) | ->]; [right; reflexivity | exact Ht1].
Qed.

Lemma relabel_fwd : fwd_conds relabel.
Proof.
  refine (conj _ (conj _ (conj _ _))).
  - intros e e' c es cs H Hs. apply prog_step_inv in Hs. destruct Hs as [em [Hs Hcc]].
    destruct (relabel_step _ _ _ H _ _ _ Hs Hcc) as (e's & em' & Hs' & Hcc' & Hr).
    exists e's. split; [eapply PS_step; [eapply P_Step; eauto | apply PS_refl] | exact Hr].
  - intros e e' c f ep H Hf. eapply relabel_feed; eauto.
  - intros e e' c π b b' t o c' H Hg Hs. eapply relabel_ctx; eauto.
  - exact relabel_done.
Qed.

(* In reverse, the relabeling rules never lag: both contexts stay equal. *)
Definition relabel_diag (e e' : Expr) (cL cR : Chan) : Prop := cL = cR /\ relabel e e' cL.

Lemma relabel_rev : rev_conds relabel_diag.
Proof.
  refine (conj _ (conj _ (conj _ _))).
  - intros e e' cL cR e's cR' [<- H] _ Hs. apply prog_step_inv in Hs. destruct Hs as [em' [Hs Hcc]].
    destruct (relabel_step_rev _ _ _ H _ _ _ Hs Hcc) as (es & em & Hs' & Hcc' & Hr).
    exists es, cR'. split; [eapply PS_step; [eapply P_Step; eauto | apply PS_refl] | split; [reflexivity | exact Hr]].
  - intros e e' cL cR f ep' τ ci [<- H] Hcal Hinp Hsub Hf.
    destruct (relabel_feed_rev _ _ _ H _ _ _ _ Hcal Hinp Hsub Hf) as [ep Hfe].
    destruct (relabel_feed _ _ _ H _ _ Hfe) as [ep'' [Hf'' Hr]].
    rewrite Hf in Hf''. injection Hf'' as <-.
    exists ep. split; [exact Hfe | split; [reflexivity | exact Hr]].
  - intros e e' cL cR π v t o cR' [<- H] (A & B & Hg & Ho & Hs).
    exists cR'. split; [exists A, B; auto | split; [reflexivity | eapply relabel_ctx; eauto]].
  - intros e e' cL cR [<- H] Hd. split; [eapply relabel_done_rev; eauto | apply chan_equiv_refl].
Qed.

Lemma lag_diag_eq : forall e e' cL cR, lag_rel relabel_diag e e' cL cR -> cL = cR.
Proof.
  intros e e' cL cR H. induction H; [destruct H; assumption | congruence | congruence | reflexivity | assumption].
Qed.

(* ============================================================================
   18. Soundness.
   ========================================================================== *)

(* This file's transformations preserve every reachable output channel, and
   every final output, exactly. *)
Theorem soundness :
  forall e e' c,
    transform0 e e' c ->
    wt_out e c ->
    forall c_final, reaches_out e c c_final <-> reaches_out e' c c_final.
Proof.
  intros e e' c Htr Hwt c_final.
  destruct relabel_fwd as (Fs & Ff & Fc & Fd). destruct relabel_rev as (Rs & Rf & Rc & Rd).
  split.
  - intros [ef Hss].
    destruct (fwd_sim_star relabel Fs Ff Fc _ _ Hss _ _ _ _ _ eq_refl eq_refl Htr) as [e'f [Hss' _]].
    exists e'f. exact Hss'.
  - intros [e'f Hss].
    destruct (lag_sim_star relabel_diag Rs Rf Rc _ _ Hss _ _ _ _ _ _ eq_refl eq_refl
                (transform_to_lag _ _ (fun e e' c H => conj eq_refl H) _ _ _ Htr) Hwt) as [ef [cLf [Hss' Hr]]].
    rewrite (lag_diag_eq _ _ _ _ Hr) in Hss'. exists ef. exact Hss'.
Qed.

Theorem fin_soundness :
  forall e e' c,
    transform0 e e' c ->
    wt_out e c ->
    forall c_final, fin_output e c c_final <-> fin_output e' c c_final.
Proof.
  intros e e' c Htr Hwt c_final.
  destruct relabel_fwd as (Fs & Ff & Fc & Fd). destruct relabel_rev as (Rs & Rf & Rc & Rd).
  split.
  - intros [ef [Hss Hd]].
    destruct (fwd_sim_star relabel Fs Ff Fc _ _ Hss _ _ _ _ _ eq_refl eq_refl Htr) as [e'f [Hss' Hr]].
    exists e'f. split; [exact Hss' | eapply fwd_done; eauto].
  - intros [e'f [Hss Hd]].
    destruct (lag_sim_star relabel_diag Rs Rf Rc _ _ Hss _ _ _ _ _ _ eq_refl eq_refl
                (transform_to_lag _ _ (fun e e' c H => conj eq_refl H) _ _ _ Htr) Hwt) as [ef [cLf [Hss' Hr]]].
    destruct (lag_done relabel_diag Rd _ _ _ _ Hr Hd) as [Hdf _].
    rewrite (lag_diag_eq _ _ _ _ Hr) in Hss'. exists ef. split; assumption.
Qed.

(* With extra base rewrites B (e.g. partitioning), final outputs are preserved
   up to chan_equiv. *)
Theorem ext_fin_soundness :
  forall (B Bf : Expr -> Expr -> Chan -> Prop) (Br : Expr -> Expr -> Chan -> Chan -> Prop),
    (forall e e' c, B e e' c -> Bf e e' c) ->
    (forall e e' c, B e e' c -> Br e e' c c) ->
    fwd_conds Bf -> rev_conds Br ->
    forall e e' c,
      transform (rw_ext B) e e' c ->
      wt_out e c ->
      forall c_final,
        (fin_output e c c_final -> exists c', fin_output e' c c' /\ chan_equiv c_final c') /\
        (fin_output e' c c_final -> exists c', fin_output e c c' /\ chan_equiv c' c_final).
Proof.
  intros B Bf Br HBf HBr HF HR.
  apply (transform_fin_soundness (rw_ext B) (fun e e' c => relabel e e' c \/ Bf e e' c)
           (fun e e' cL cR => relabel_diag e e' cL cR \/ Br e e' cL cR)).
  - intros e e' c [H|H]; [left | right]; auto.
  - intros e e' c [H|H]; [left; split; auto | right; auto].
  - apply fwd_conds_union; [exact relabel_fwd | exact HF].
  - apply rev_conds_union; [exact relabel_rev | exact HR].
Qed.

Corollary Trans_Type_preservation0 :
  forall e e' c, wt_out e c -> transform0 e e' c -> wt_out e' c.
Proof. exact (Trans_Type_preservation relabel relabel_typing). Qed.
