(* ============================================================================
   Partitioning.v

   Coq/Rocq formalization of the "Partitioning" section of
     "Sound Optimization Transformations for Distributed Systems".

   We build on Transformations.v and formalize the partitioned kop (pkop) and
   the plain kop, where kop is any (potentially stateful) operator whose
   processing can be partitioned into keyed regions:

         kop   ↣   split ; (kop | kop) ; merge

   The rewrite (partition) is a base rewrite added to Transformations.v's own
   (transform (rw_ext partition)), so it applies anywhere inside a program,
   alongside the relabeling rules, for a kop whose input channels and output
   channel are Unordered.

   The whole development is parametric in a single location l (the paper leaves
   the location of kop/split/merge implicit; it is uniformly l).

   The kop is a keyed state machine: per key it keeps a KOp (logic + state,
   like Op), and on a value v it runs kapply_op on v's key's KOp only.  KOp and
   kapply_op are arbitrary, so the results hold for every such operator
   (single-input operators are fed  〈b, []〉).

   Main results (Qed; no Admitted):
     - Partitioning : transform (rw_ext partition) e e' c -> wt_out e c ->
         every final output of e is, up to chan_equiv, a final output of e',
         and vice versa.
     - Partitioning_typing : wt_out e c -> transform (rw_ext partition) e e' c ->
         wt_out e' c.
   It is ext_fin_soundness of Transformations.v, instantiated with two
   simulations:
     - part_fwd (kop → pkop): pkop routes each value the kop processes through
       the pipeline and delivers its outputs, so both sides share the output
       context;
     - part_rev (pkop → kop): the kop processes a value exactly when a
       partition kop does, so the kop's output context runs ahead of pkop's by
       the values still in the merge — a difference only in an Unordered
       buffer.
   ========================================================================== *)

From Stdlib Require Import List.
Import ListNotations.
From Stdlib Require Import Permutation.
Require Import Transformations.

(* ============================================================================
   1. Parameters of the Partitioning section.
   ========================================================================== *)

(* The (implicit) location at which all operators live. *)
Parameter l : Loc.

(* Keys and the key function key : V → K. *)
Parameter K : Type.
Parameter key : Val -> K.
Parameter K_eq_dec : forall k1 k2 : K, {k1 = k2} + {k1 <> k2}.

(* The partition  K = K₁ ⊎ K₂.  [in_K1 k = true]  iff  k ∈ K₁;
   K₂ = K ∖ K₁ is its complement. *)
Parameter in_K1 : K -> bool.

(* Keyed operator, including state *)
Parameter KOp : Type.

(* Single step for a particular key. Note that it takes
   Dir instead of Path because it must have exactly 2 inputs. Single-input operators
 can be modeled by having one input be empty. *)
Parameter kapply_op : KOp -> Dir -> Val -> KOp * Buffer.

(* Map from key to their KOp. *)
Definition KMap : Type := K -> KOp.

(* Update the KMap at key k with its new state kop. *)
Definition kupdate (km : KMap) (k : K) (kop : KOp) : KMap :=
  fun k' => if K_eq_dec k k' then kop else km k'.

(* ============================================================================
   2. The three operators as abstract Op values, with their apply_op behaviour
      axiomatized to match the paper's definitions.
   ========================================================================== *)

Parameter o_split : Op.
Parameter o_merge : Op.
Parameter o_kop : KMap -> Op.

(* The 2×2 buffer tree holding just v, at position [d1; d2]. *)
Definition emit2x2 (d1 d2 : Dir) (v : Val) : BufTree :=
  match d1, d2 with
  | DL, DL => BPair (BPair (BLeaf [v]) (BLeaf [])) (BPair (BLeaf []) (BLeaf []))
  | DL, DR => BPair (BPair (BLeaf []) (BLeaf [v])) (BPair (BLeaf []) (BLeaf []))
  | DR, DL => BPair (BPair (BLeaf []) (BLeaf [])) (BPair (BLeaf [v]) (BLeaf []))
  | DR, DR => BPair (BPair (BLeaf []) (BLeaf [])) (BPair (BLeaf []) (BLeaf [v]))
  end.

(* The partition of v: L for K₁, R for K₂. *)
Definition part_of (v : Val) : Dir := if in_K1 (key v) then DL else DR.

(* split([d], v) ≔ ⟨split, emit([p; d], v)⟩, where p is v's partition.  split
   reads both inputs and routes each value to its partition, keeping its input
   side: its output tree is indexed partition-then-side, the input of
   (kop | kop). *)
Axiom apply_split :
  forall d v, apply_op o_split [d] v = (o_split, emit2x2 (part_of v) d v).

(* merge(π,v) ≔ ⟨merge, v⟩ : pass each value through to the single output. *)
Axiom apply_merge :
  forall π v, apply_op o_merge π v = (o_merge, BLeaf [v]).

(* Applying a kmap (which contains all kops for all keys)
   means finding the key of the value, finding the kop for that key,
   evaluating the kop and generating kop', then updating the kmap *)
Axiom apply_kop :
  forall km d v kop' out,
  kapply_op (km (key v)) d v = (kop', out) ->
  apply_op (o_kop km) [d] v = (o_kop (kupdate km (key v) kop'), BLeaf out).

(* Signatures and input modes of split and merge (the kop's signature is
   forced by its typing in context).  Neither needs ordered input. *)
Axiom sig_split : Op_sig o_split = (STPair STLeaf STLeaf,
                                    STPair (STPair STLeaf STLeaf) (STPair STLeaf STLeaf)).
Axiom sig_merge : Op_sig o_merge = (STPair STLeaf STLeaf, STLeaf).
Axiom mode_split : fst (Op_mode o_split) = AnyOrder.
Axiom mode_merge : fst (Op_mode o_merge) = AnyOrder.

(* ============================================================================
   3. Stages.

      Partitioning applies to a kop whose input and output channels are
      Unordered.  Every buffer inside the partitioned pipeline is a local,
      Unordered buffer at l, except the split's inputs, which are the kop's.
   ========================================================================== *)

(* A local, Unordered buffer. *)
Definition cb (b : Buffer) : Chan := CBuf b (TLoc l) Unordered.

(* A kop with left / right inputs B1, B2 (location types t1, t2) and state km. *)
Definition kconf (B1 : Buffer) (t1 : CType) (B2 : Buffer) (t2 : CType) (km : KMap) : Expr :=
  EOp (CPair (CBuf B1 t1 Unordered) (CBuf B2 t2 Unordered)) (o_kop km) l.

(* A partition's kop, inside the pipeline. *)
Definition kpart (in_L in_R : Buffer) (km : KMap) : Expr := kconf in_L (TLoc l) in_R (TLoc l) km.

(* The split, with the kop's inputs. *)
Definition split_st (in_L : Buffer) (t1 : CType) (in_R : Buffer) (t2 : CType) : Expr :=
  EOp (CPair (CBuf in_L t1 Unordered) (CBuf in_R t2 Unordered)) o_split l.

(* The merge, with inputs from the two partitions. *)
Definition merge_st (in_K1 in_K2 : Buffer) : Expr :=
  EOp (CPair (cb in_K1) (cb in_K2)) o_merge l.

(* The pipeline  split ; (kop | kop) ; merge. *)
Definition pipe (split kop1 kop2 merge : Expr) : Expr :=
  ESeq (ESeq split (EPar kop1 kop2)) merge.

(* The partitioned kop replacing kconf B1 t1 B2 t2 km: both partitions start
   from km (each only ever uses its own keys). *)
Definition pkop_of (B1 : Buffer) (t1 : CType) (B2 : Buffer) (t2 : CType) (km : KMap) : Expr :=
  pipe (split_st B1 t1 B2 t2) (kpart [] [] km) (kpart [] [] km) (merge_st [] []).

(* ============================================================================
   4. Helpers.
   ========================================================================== *)

Lemma prog_star_one : forall x y, prog_step x y -> prog_star x y.
Proof. intros x y H. eapply PS_step; [exact H | apply PS_refl]. Qed.

(* Resolve the location check of chan_concat on two buffers at l. *)
Ltac loc_refl :=
  repeat match goal with
  | |- context [Loc_eq_dec l l] =>
      let E := fresh in destruct (Loc_eq_dec l l) as [_|E]; [|exfalso; apply E; reflexivity]
  end.

(* A location an operator at l can read from or emit to. *)
Definition loc_ok (t : CType) : Prop := t = TLoc l \/ t = TNet.

(* The output context of a kop that can be partitioned: one Unordered buffer. *)
Definition out_ok (c : Chan) : Prop := exists b t, c = CBuf b t Unordered /\ loc_ok t.

Lemma cc_at_l : forall x y o o' t,
  loc_ok t -> chan_concat (CBuf x (TLoc l) o) (CBuf y t o') = Some (CBuf (x ++ y) t o').
Proof. intros x y o o' t [-> | ->]; simpl; loc_refl; reflexivity. Qed.

(* Appending to one buffer, whatever its contents. *)
Lemma concat_buf_any : forall f X t o r,
  chan_concat f (CBuf X t o) = Some r ->
  exists x, r = CBuf (x ++ X) t o /\ forall Y, chan_concat f (CBuf Y t o) = Some (CBuf (x ++ Y) t o).
Proof.
  intros [|? ?|x tx ox] X t o r H; simpl in H; try discriminate.
  - inversion H; subst. exists []. split; [reflexivity | intros Y; reflexivity].
  - destruct (ctype_combine tx t) as [t'|] eqn:E; try discriminate.
    inversion H; subst. rewrite (ctype_combine_r _ _ _ E).
    exists x. split; [reflexivity|]. intros Y. simpl. rewrite E, (ctype_combine_r _ _ _ E). reflexivity.
Qed.

(* Where a buffer can sit in the channels used here. *)
Lemma chan_get_buf : forall X t1 o1 π Y t o,
  chan_get (CBuf X t1 o1) π = Some (CBuf Y t o) -> π = [] /\ Y = X /\ t = t1 /\ o = o1.
Proof.
  intros X t1 o1 [|[|] π] Y t o H; simpl in H; try discriminate. inversion H; subst; auto.
Qed.

Lemma chan_get_pair : forall X1 t1 o1 X2 t2 o2 π Y t o,
  chan_get (CPair (CBuf X1 t1 o1) (CBuf X2 t2 o2)) π = Some (CBuf Y t o) ->
  (π = [DL] /\ Y = X1 /\ t = t1 /\ o = o1) \/ (π = [DR] /\ Y = X2 /\ t = t2 /\ o = o2).
Proof.
  intros X1 t1 o1 X2 t2 o2 [|[|] [|[|] π]] Y t o H; simpl in H; try discriminate;
    inversion H; subst; auto.
Qed.

(* Every buffer of c is empty, computed structurally. *)
Fixpoint bufs_empty (c : Chan) : Prop :=
  match c with
  | CEps => True
  | CBuf b _ _ => b = []
  | CPair a b => bufs_empty a /\ bufs_empty b
  end.

Lemma bufs_empty_buf_in : forall c, bufs_empty c -> forall b t o, buf_in b t o c -> b = [].
Proof.
  induction c as [|a IHa b IHb|bf tf oo]; simpl; intros H b0 t0 o0 Hin.
  - destruct Hin as [[|[|] π] Hg]; discriminate.
  - destruct H as [Ha Hb]. destruct (buf_in_pair _ _ _ _ _ Hin) as [Hi|Hi]; eauto.
  - destruct (buf_in_cbuf _ _ _ _ _ _ Hin) as (-> & _ & _). exact H.
Qed.

Lemma buf_in_bufs_empty : forall c, (forall b t o, buf_in b t o c -> b = []) -> bufs_empty c.
Proof.
  induction c as [|a IHa b IHb|bf tf oo]; simpl; intros H.
  - exact I.
  - split; [apply IHa | apply IHb]; intros x y z [π Hg]; eapply H;
      [exists (DL :: π) | exists (DR :: π)]; exact Hg.
  - eapply H. exists []. reflexivity.
Qed.

Lemma done_op : forall c o l', bufs_empty c -> done (EOp c o l').
Proof. intros c o l' H. constructor. apply bufs_empty_buf_in. exact H. Qed.

Lemma done_op_inv : forall c o l', done (EOp c o l') -> bufs_empty c.
Proof. intros c o l' H. inversion H; subst. apply buf_in_bufs_empty. assumption. Qed.

(* psolve proves  Permutation X Y  where X and Y concatenate the same buffers
   (as a multiset of syntactic atoms; a cons v :: R counts as [v] ++ R).  It
   cancels the first atom of X against an occurrence in Y, rotating Y until
   that occurrence is in front. *)
Lemma cons_app_single : forall (A : Type) (v : A) (R : list A), v :: R = [v] ++ R.
Proof. reflexivity. Qed.

Lemma perm_rot : forall (A B C : list Val), Permutation A (C ++ B) -> Permutation A (B ++ C).
Proof. intros A B C H. eapply Permutation_trans; [exact H | apply Permutation_app_comm]. Qed.

Ltac cons_to_app :=
  repeat match goal with
  | |- context [?v :: ?R] =>
      lazymatch R with
      | [] => fail
      | _ => rewrite (cons_app_single _ v R)
      end
  end.

Ltac pnorm := repeat rewrite <- app_assoc; rewrite ?app_nil_l, ?app_nil_r.

Ltac rot_to a :=
  do 40 (try (lazymatch goal with
              | |- Permutation _ (a ++ _) => fail
              | |- Permutation _ (_ ++ _) => apply perm_rot; pnorm
              end)).

Ltac psolve_core :=
  lazymatch goal with
  | |- Permutation ?X ?X => apply Permutation_refl
  | |- Permutation (?a ++ _) _ =>
      rot_to a;
      lazymatch goal with
      | |- Permutation (a ++ _) (a ++ _) => apply Permutation_app_head; psolve_core
      end
  end.

Ltac psolve := cons_to_app; pnorm; psolve_core.

(* Membership of all a buffer's values in one partition, a bag property. *)
Definition all_in (P : Val -> bool) (b : Buffer) : Prop := forall v, In v b -> P v = true.

Lemma all_in_perm : forall P b b', all_in P b -> Permutation b b' -> all_in P b'.
Proof.
  intros P b b' H Hp v Hv. apply H. eapply Permutation_in; [apply Permutation_sym; exact Hp | exact Hv].
Qed.

Lemma all_in_cons : forall P v b, P v = true -> all_in P b -> all_in P (v :: b).
Proof. intros P v b Hv H x [<-|Hx]; auto. Qed.

Lemma all_in_nil : forall P, all_in P [].
Proof. intros P v []. Qed.

(* A value of a permuted buffer can be removed from it. *)
Lemma perm_pick : forall (b R : Buffer) x,
  Permutation b (x :: R) -> exists A B, b = A ++ x :: B /\ Permutation (A ++ B) R.
Proof.
  intros b R x H.
  assert (Hin : In x b) by (eapply Permutation_in; [apply Permutation_sym; exact H | left; reflexivity]).
  destruct (in_split x b Hin) as (A & B & ->).
  exists A, B. split; [reflexivity|].
  apply Permutation_sym. eapply Permutation_cons_app_inv. apply Permutation_sym. exact H.
Qed.

(* Partition membership of a value: kk1 = in K1, kk2 = in K2. *)
Definition kk1 (v : Val) : bool := in_K1 (key v).
Definition kk2 (v : Val) : bool := negb (in_K1 (key v)).

Lemma kk1_kk2_f : forall v, kk1 v = false -> kk2 v = true.
Proof. unfold kk1, kk2. intros v H. rewrite H. reflexivity. Qed.

Lemma kk2_kk1_t : forall v, kk2 v = true -> kk1 v = false.
Proof. unfold kk1, kk2. intros v H. destruct (in_K1 (key v)); [discriminate | reflexivity]. Qed.

(* A partition's kmap agrees with the plain kop's on the partition's keys. *)
Definition agree (p : bool) (km_part km : KMap) : Prop := forall k, in_K1 k = p -> km_part k = km k.

Lemma agree_upd_in : forall p km_part km k x,
  agree p km_part km -> agree p (kupdate km_part k x) (kupdate km k x).
Proof. intros p km_part km k x H k' Hk'. unfold kupdate. destruct (K_eq_dec k k'); auto. Qed.

Lemma agree_upd_out : forall p km_part km v x,
  agree p km_part km -> in_K1 (key v) <> p -> agree p km_part (kupdate km (key v) x).
Proof.
  intros p km_part km v x H Hne k Hk. unfold kupdate.
  destruct (K_eq_dec (key v) k) as [E|E]; [subst; contradiction | apply H; exact Hk].
Qed.

(* ============================================================================
   5. The stages' steps.
   ========================================================================== *)

Ltac unord := let H := fresh in intro H; discriminate H.

Lemma kop_step_inv : forall B1 t1 B2 t2 km e' c',
  step (kconf B1 t1 B2 t2 km) e' c' ->
  (exists A v B kop' out, B1 = A ++ v :: B /\ kapply_op (km (key v)) DL v = (kop', out) /\
     e' = kconf (A ++ B) t1 B2 t2 (kupdate km (key v) kop') /\ c' = CBuf out (TLoc l) Unordered) \/
  (exists A v B kop' out, B2 = A ++ v :: B /\ kapply_op (km (key v)) DR v = (kop', out) /\
     e' = kconf B1 t1 (A ++ B) t2 (kupdate km (key v) kop') /\ c' = CBuf out (TLoc l) Unordered).
Proof.
  intros B1 t1 B2 t2 km e' c' H. unfold kconf in H. apply step_EOp_form in H.
  destruct H as (π & v & t & ord & A & B & c'' & o' & β & Hg & _ & _ & Hs & Hap & -> & ->).
  destruct (chan_get_pair _ _ _ _ _ _ _ _ _ _ Hg) as [(-> & <- & -> & ->) | (-> & <- & -> & ->)].
  - left. destruct (kapply_op (km (key v)) DL v) as [kop' out] eqn:Hk.
    rewrite (apply_kop _ _ _ _ _ Hk) in Hap. inversion Hap; subst o' β.
    simpl in Hs. inversion Hs; subst c''.
    exists A, v, B, kop', out. repeat split. assumption.
  - right. destruct (kapply_op (km (key v)) DR v) as [kop' out] eqn:Hk.
    rewrite (apply_kop _ _ _ _ _ Hk) in Hap. inversion Hap; subst o' β.
    simpl in Hs. inversion Hs; subst c''.
    exists A, v, B, kop', out. repeat split. assumption.
Qed.

Lemma kop_step_L : forall A v B t1 B2 t2 km kop' out,
  loc_ok t1 -> kapply_op (km (key v)) DL v = (kop', out) ->
  step (kconf (A ++ v :: B) t1 B2 t2 km) (kconf (A ++ B) t1 B2 t2 (kupdate km (key v) kop'))
       (CBuf out (TLoc l) Unordered).
Proof.
  intros A v B t1 B2 t2 km kop' out Ht Hk. unfold kconf.
  eapply S_Op with (π := [DL]) (v := v) (t := t1) (ord := Unordered) (b1 := A) (b2 := B) (β := BLeaf out);
    [reflexivity | unord | exact Ht | reflexivity | apply apply_kop; exact Hk].
Qed.

Lemma kop_step_R : forall A v B B1 t1 t2 km kop' out,
  loc_ok t2 -> kapply_op (km (key v)) DR v = (kop', out) ->
  step (kconf B1 t1 (A ++ v :: B) t2 km) (kconf B1 t1 (A ++ B) t2 (kupdate km (key v) kop'))
       (CBuf out (TLoc l) Unordered).
Proof.
  intros A v B B1 t1 t2 km kop' out Ht Hk. unfold kconf.
  eapply S_Op with (π := [DR]) (v := v) (t := t2) (ord := Unordered) (b1 := A) (b2 := B) (β := BLeaf out);
    [reflexivity | unord | exact Ht | reflexivity | apply apply_kop; exact Hk].
Qed.

Lemma part_of_K1 : forall v, kk1 v = true -> part_of v = DL.
Proof. unfold kk1, part_of. intros v H. rewrite H. reflexivity. Qed.

Lemma part_of_K2 : forall v, kk1 v = false -> part_of v = DR.
Proof. unfold kk1, part_of. intros v H. rewrite H. reflexivity. Qed.

Lemma split_step_inv : forall in_L t1 in_R t2 e' c',
  step (split_st in_L t1 in_R t2) e' c' ->
  (exists A v B, in_L = A ++ v :: B /\ e' = split_st (A ++ B) t1 in_R t2 /\
     c' = wrap_loc (emit2x2 (part_of v) DL v) (TLoc l)) \/
  (exists A v B, in_R = A ++ v :: B /\ e' = split_st in_L t1 (A ++ B) t2 /\
     c' = wrap_loc (emit2x2 (part_of v) DR v) (TLoc l)).
Proof.
  intros in_L t1 in_R t2 e' c' H. unfold split_st in H. apply step_EOp_form in H.
  destruct H as (π & v & t & ord & A & B & c'' & o' & β & Hg & _ & _ & Hs & Hap & -> & ->).
  destruct (chan_get_pair _ _ _ _ _ _ _ _ _ _ Hg) as [(-> & <- & -> & ->) | (-> & <- & -> & ->)];
    rewrite apply_split in Hap; inversion Hap; subst o' β; simpl in Hs; inversion Hs; subst c''.
  - left. exists A, v, B. repeat split.
  - right. exists A, v, B. repeat split.
Qed.

Lemma split_op_L : forall A v B t1 in_R t2,
  loc_ok t1 ->
  step (split_st (A ++ v :: B) t1 in_R t2) (split_st (A ++ B) t1 in_R t2)
       (wrap_loc (emit2x2 (part_of v) DL v) (TLoc l)).
Proof.
  intros A v B t1 in_R t2 Ht. unfold split_st.
  eapply S_Op with (π := [DL]) (v := v) (t := t1) (ord := Unordered) (b1 := A) (b2 := B);
    [reflexivity | unord | exact Ht | reflexivity | apply apply_split].
Qed.

Lemma split_op_R : forall in_L t1 A v B t2,
  loc_ok t2 ->
  step (split_st in_L t1 (A ++ v :: B) t2) (split_st in_L t1 (A ++ B) t2)
       (wrap_loc (emit2x2 (part_of v) DR v) (TLoc l)).
Proof.
  intros in_L t1 A v B t2 Ht. unfold split_st.
  eapply S_Op with (π := [DR]) (v := v) (t := t2) (ord := Unordered) (b1 := A) (b2 := B);
    [reflexivity | unord | exact Ht | reflexivity | apply apply_split].
Qed.

Lemma merge_step_inv : forall g1 g2 e' c',
  step (merge_st g1 g2) e' c' ->
  (exists A v B, g1 = A ++ v :: B /\ e' = merge_st (A ++ B) g2 /\ c' = CBuf [v] (TLoc l) Unordered) \/
  (exists A v B, g2 = A ++ v :: B /\ e' = merge_st g1 (A ++ B) /\ c' = CBuf [v] (TLoc l) Unordered).
Proof.
  intros g1 g2 e' c' H. unfold merge_st in H. apply step_EOp_form in H.
  destruct H as (π & v & t & ord & A & B & c'' & o' & β & Hg & _ & _ & Hs & Hap & -> & ->).
  destruct (chan_get_pair _ _ _ _ _ _ _ _ _ _ Hg) as [(-> & <- & -> & ->) | (-> & <- & -> & ->)];
    rewrite apply_merge in Hap; inversion Hap; subst o' β; simpl in Hs; inversion Hs; subst c''.
  - left. exists A, v, B. repeat split.
  - right. exists A, v, B. repeat split.
Qed.

Lemma merge_op_K1 : forall A v B g2,
  step (merge_st (A ++ v :: B) g2) (merge_st (A ++ B) g2) (CBuf [v] (TLoc l) Unordered).
Proof.
  intros. unfold merge_st.
  eapply S_Op with (π := [DL]) (v := v) (t := TLoc l) (ord := Unordered) (b1 := A) (b2 := B) (β := BLeaf [v]);
    [reflexivity | unord | left; reflexivity | reflexivity | apply apply_merge].
Qed.

Lemma merge_op_K2 : forall A v B g1,
  step (merge_st g1 (A ++ v :: B)) (merge_st g1 (A ++ B)) (CBuf [v] (TLoc l) Unordered).
Proof.
  intros. unfold merge_st.
  eapply S_Op with (π := [DR]) (v := v) (t := TLoc l) (ord := Unordered) (b1 := A) (b2 := B) (β := BLeaf [v]);
    [reflexivity | unord | left; reflexivity | reflexivity | apply apply_merge].
Qed.

(* ---- feeding one stage's emission into the next ---- *)

(* split's emission reaches one input of one partition's kop. *)
Lemma feed_split : forall v a b km c d km',
  feed (wrap_loc (emit2x2 DL DL v) (TLoc l)) (EPar (kpart a b km) (kpart c d km'))
    = Some (EPar (kpart (v :: a) b km) (kpart c d km')) /\
  feed (wrap_loc (emit2x2 DL DR v) (TLoc l)) (EPar (kpart a b km) (kpart c d km'))
    = Some (EPar (kpart a (v :: b) km) (kpart c d km')) /\
  feed (wrap_loc (emit2x2 DR DL v) (TLoc l)) (EPar (kpart a b km) (kpart c d km'))
    = Some (EPar (kpart a b km) (kpart (v :: c) d km')) /\
  feed (wrap_loc (emit2x2 DR DR v) (TLoc l)) (EPar (kpart a b km) (kpart c d km'))
    = Some (EPar (kpart a b km) (kpart c (v :: d) km')).
Proof. intros. unfold kpart, kconf. repeat split; simpl; loc_refl; reflexivity. Qed.

Lemma feed_kop1_merge : forall out g1 g2,
  feed (CPair (CBuf out (TLoc l) Unordered) CEps) (merge_st g1 g2) = Some (merge_st (out ++ g1) g2).
Proof. intros. unfold merge_st, cb. simpl. loc_refl. reflexivity. Qed.

Lemma feed_kop2_merge : forall out g1 g2,
  feed (CPair CEps (CBuf out (TLoc l) Unordered)) (merge_st g1 g2) = Some (merge_st g1 (out ++ g2)).
Proof. intros. unfold merge_st, cb. simpl. loc_refl. reflexivity. Qed.

(* ============================================================================
   6. Pipeline steps: one stage at a time.
   ========================================================================== *)

Lemma pipe_step_inv : forall s k1 k2 g c e' c'',
  prog_step (pipe s k1 k2 g, c) (e', c'') ->
  (exists s' cs ks', step s s' cs /\ feed cs (EPar k1 k2) = Some ks' /\
     e' = ESeq (ESeq s' ks') g /\ c'' = c) \/
  (exists k1' ck g', step k1 k1' ck /\ feed (CPair ck CEps) g = Some g' /\
     e' = pipe s k1' k2 g' /\ c'' = c) \/
  (exists k2' ck g', step k2 k2' ck /\ feed (CPair CEps ck) g = Some g' /\
     e' = pipe s k1 k2' g' /\ c'' = c) \/
  (exists g' cg, step g g' cg /\ chan_concat cg c = Some c'' /\
     e' = pipe s k1 k2 g').
Proof.
  intros s k1 k2 g c e' c'' H.
  apply prog_step_inv in H. destruct H as [ci [H Hcc]]. unfold pipe in H.
  apply step_ESeq_form in H.
  destruct H as [ (e1 & c1 & g' & H & Hfg & -> & ->) | (g' & cg & H & -> & ->) ].
  - simpl in Hcc. injection Hcc as <-.
    apply step_ESeq_form in H.
    destruct H as [ (s' & cs & ks' & H & Hfk & -> & ->) | (ks' & ck & H & -> & ->) ].
    + rewrite feed_CEps_identity in Hfg. injection Hfg as <-.
      left. exists s', cs, ks'. auto.
    + apply step_EPar_form in H.
      destruct H as [ (k1' & ck' & H & -> & ->) | (k2' & ck' & H & -> & ->) ].
      * right; left. exists k1', ck', g'. auto.
      * do 2 right; left. exists k2', ck', g'. auto.
  - do 3 right. exists g', cg. auto.
Qed.

Lemma lift_split : forall s s' cs k1 k2 k1' k2' g c,
  step s s' cs -> feed cs (EPar k1 k2) = Some (EPar k1' k2') ->
  prog_step (pipe s k1 k2 g, c) (pipe s' k1' k2' g, c).
Proof.
  intros. unfold pipe. apply P_Step with (c' := CEps); [|reflexivity].
  eapply S_Seq_L; [|apply feed_CEps_identity].
  eapply S_Seq_L; eassumption.
Qed.

Lemma lift_kop1 : forall s k1 k1' ck k2 g g' c,
  step k1 k1' ck -> feed (CPair ck CEps) g = Some g' ->
  prog_step (pipe s k1 k2 g, c) (pipe s k1' k2 g', c).
Proof.
  intros. unfold pipe. apply P_Step with (c' := CEps); [|reflexivity].
  eapply S_Seq_L; [apply S_Seq_R; apply S_Par_L; eassumption | eassumption].
Qed.

Lemma lift_kop2 : forall s k1 k2 k2' ck g g' c,
  step k2 k2' ck -> feed (CPair CEps ck) g = Some g' ->
  prog_step (pipe s k1 k2 g, c) (pipe s k1 k2' g', c).
Proof.
  intros. unfold pipe. apply P_Step with (c' := CEps); [|reflexivity].
  eapply S_Seq_L; [apply S_Seq_R; apply S_Par_R; eassumption | eassumption].
Qed.

Lemma lift_merge : forall s k1 k2 g g' cg c c',
  step g g' cg -> chan_concat cg c = Some c' ->
  prog_step (pipe s k1 k2 g, c) (pipe s k1 k2 g', c').
Proof.
  intros. unfold pipe. eapply P_Step; [apply S_Seq_R; eassumption | eassumption].
Qed.

(* ============================================================================
   7. The partitioning rewrite and its two simulation relations.
   ========================================================================== *)

(* kconf B1 t1 B2 t2 km ↣ pkop_of B1 t1 B2 t2 km, for a kop whose input
   buffers (and output context) are Unordered. *)
Definition partition (e e' : Expr) (c : Chan) : Prop :=
  exists B1 t1 B2 t2 km,
    e = kconf B1 t1 B2 t2 km /\ e' = pkop_of B1 t1 B2 t2 km /\
    loc_ok t1 /\ loc_ok t2 /\ out_ok c.

(* The pipeline with nothing in flight between the split and the merge. *)
Definition prouted (split_L : Buffer) (t1 : CType) (split_R : Buffer) (t2 : CType)
                   (km1 km2 : KMap) (merge_K1 merge_K2 : Buffer) : Expr :=
  pipe (split_st split_L t1 split_R t2) (kpart [] [] km1) (kpart [] [] km2) (merge_st merge_K1 merge_K2).

(* The pipeline with no value in flight. *)
Definition pclean (split_L : Buffer) (t1 : CType) (split_R : Buffer) (t2 : CType)
                  (km1 km2 : KMap) : Expr :=
  prouted split_L t1 split_R t2 km1 km2 [] [].

(* Forward: after each kop step, pkop has routed the processed value and
   delivered all its outputs, so both sides share the output context.  Each
   partition's kmap agrees with the kop's on the partition's keys. *)
Definition part_fwd (e e' : Expr) (c : Chan) : Prop :=
  exists B1 t1 B2 t2 km km1 km2,
    e = kconf B1 t1 B2 t2 km /\ e' = pclean B1 t1 B2 t2 km1 km2 /\
    loc_ok t1 /\ loc_ok t2 /\ out_ok c /\ agree true km1 km /\ agree false km2 km.

(* Reverse: kop has processed exactly the values pkop's partitions have.  Its
   unprocessed inputs are those still in the split or on their way to a
   partition kop; its output context out_L is pkop's out_R plus the values
   still in the merge. *)
Definition part_rev (e e' : Expr) (cL cR : Chan) : Prop :=
  exists B1 t1 B2 t2 km split_L split_R kop1_L kop1_R km1 kop2_L kop2_R km2
         merge_K1 merge_K2 out_L out_R tc,
    e = kconf B1 t1 B2 t2 km /\
    e' = pipe (split_st split_L t1 split_R t2)
              (kpart kop1_L kop1_R km1) (kpart kop2_L kop2_R km2) (merge_st merge_K1 merge_K2) /\
    cL = CBuf out_L tc Unordered /\ cR = CBuf out_R tc Unordered /\
    loc_ok t1 /\ loc_ok t2 /\ loc_ok tc /\
    Permutation B1 (split_L ++ kop1_L ++ kop2_L) /\
    Permutation B2 (split_R ++ kop1_R ++ kop2_R) /\
    Permutation out_L (merge_K1 ++ merge_K2 ++ out_R) /\
    all_in kk1 (kop1_L ++ kop1_R) /\ all_in kk2 (kop2_L ++ kop2_R) /\
    agree true km1 km /\ agree false km2 km.

Lemma partition_fwd : forall e e' c, partition e e' c -> part_fwd e e' c.
Proof.
  intros e e' c (B1 & t1 & B2 & t2 & km & -> & -> & Ht1 & Ht2 & Hc).
  exists B1, t1, B2, t2, km, km, km. repeat split; auto; intros k _; reflexivity.
Qed.

Lemma partition_rev : forall e e' c, partition e e' c -> part_rev e e' c c.
Proof.
  intros e e' c (B1 & t1 & B2 & t2 & km & -> & -> & Ht1 & Ht2 & (bc & tc & -> & Htc)).
  exists B1, t1, B2, t2, km, B1, B2, [], [], km, [], [], km, [], [], bc, bc, tc.
  simpl. rewrite !app_nil_r.
  repeat split; auto using Permutation_refl, all_in_nil; intros k _; reflexivity.
Qed.

(* ---- shared helpers for the proofs below ---- *)

Lemma all_in_drop : forall P L v L', all_in P L -> Permutation L (v :: L') -> all_in P L'.
Proof.
  intros P L v L' H Hp x Hx. apply H.
  eapply Permutation_in; [apply Permutation_sym; exact Hp | right; exact Hx].
Qed.

Ltac close_perm H := eapply Permutation_trans; [exact H | psolve].

Ltac close_all_in H :=
  first [ eapply all_in_perm; [exact H | psolve]
        | eapply all_in_perm; [apply all_in_cons; [eassumption | exact H] | psolve]
        | eapply all_in_perm; [apply all_in_cons; [apply kk1_kk2_f; eassumption | exact H] | psolve] ].

Lemma feed_pipe_CEps : forall s k1 k2 g,
  feed CEps (pipe s k1 k2 g) = Some (pipe s k1 k2 g).
Proof. intros. apply feed_CEps_identity. Qed.

(* Feeding a kop or the split, which have the same inputs: each input buffer
   receives a prefix, whatever it held. *)
Lemma feed_pair_inv : forall f B1 t1 B2 t2 o ep,
  feed f (EOp (CPair (CBuf B1 t1 Unordered) (CBuf B2 t2 Unordered)) o l) = Some ep ->
  f = CEps \/
  exists f1 f2 x1 x2, f = CPair f1 f2 /\
    (forall Y, chan_concat f1 (CBuf Y t1 Unordered) = Some (CBuf (x1 ++ Y) t1 Unordered)) /\
    (forall Y, chan_concat f2 (CBuf Y t2 Unordered) = Some (CBuf (x2 ++ Y) t2 Unordered)).
Proof.
  intros f B1 t1 B2 t2 o ep H. simpl in H.
  destruct f as [|f1 f2|? ? ?]; simpl in H; try discriminate; [left; reflexivity|right].
  destruct (chan_concat f1 (CBuf B1 t1 Unordered)) as [r1|] eqn:E1; try discriminate.
  destruct (chan_concat f2 (CBuf B2 t2 Unordered)) as [r2|] eqn:E2; try discriminate.
  destruct (concat_buf_any _ _ _ _ _ E1) as [x1 [_ Hx1]].
  destruct (concat_buf_any _ _ _ _ _ E2) as [x2 [_ Hx2]].
  exists f1, f2, x1, x2. auto.
Qed.

Lemma feed_kconf_pair : forall f1 f2 x1 x2 B1 t1 B2 t2 km,
  (forall Y, chan_concat f1 (CBuf Y t1 Unordered) = Some (CBuf (x1 ++ Y) t1 Unordered)) ->
  (forall Y, chan_concat f2 (CBuf Y t2 Unordered) = Some (CBuf (x2 ++ Y) t2 Unordered)) ->
  feed (CPair f1 f2) (kconf B1 t1 B2 t2 km) = Some (kconf (x1 ++ B1) t1 (x2 ++ B2) t2 km).
Proof. intros. unfold kconf. simpl. rewrite H, H0. reflexivity. Qed.

Lemma feed_pipe_pair : forall f1 f2 x1 x2 sL t1 sR t2 k1 k2 g,
  (forall Y, chan_concat f1 (CBuf Y t1 Unordered) = Some (CBuf (x1 ++ Y) t1 Unordered)) ->
  (forall Y, chan_concat f2 (CBuf Y t2 Unordered) = Some (CBuf (x2 ++ Y) t2 Unordered)) ->
  feed (CPair f1 f2) (pipe (split_st sL t1 sR t2) k1 k2 g)
    = Some (pipe (split_st (x1 ++ sL) t1 (x2 ++ sR) t2) k1 k2 g).
Proof. intros. unfold pipe, split_st. simpl. rewrite H, H0. reflexivity. Qed.

Lemma feed_pipe_inv : forall f sL t1 sR t2 k1 k2 g ep,
  feed f (pipe (split_st sL t1 sR t2) k1 k2 g) = Some ep ->
  f = CEps \/
  exists f1 f2 x1 x2, f = CPair f1 f2 /\
    (forall Y, chan_concat f1 (CBuf Y t1 Unordered) = Some (CBuf (x1 ++ Y) t1 Unordered)) /\
    (forall Y, chan_concat f2 (CBuf Y t2 Unordered) = Some (CBuf (x2 ++ Y) t2 Unordered)).
Proof.
  intros f sL t1 sR t2 k1 k2 g ep H. unfold pipe in H. cbn [feed] in H.
  destruct (feed f (split_st sL t1 sR t2)) as [s'|] eqn:Es; [|discriminate].
  exact (feed_pair_inv _ _ _ _ _ _ _ Es).
Qed.

(* ============================================================================
   8. Forward simulation: route each value the kop processes through the
      pipeline, then deliver its outputs through the merge.
   ========================================================================== *)

Lemma route_L_K1 : forall A x B t1 sR t2 km1 km2 kop' out g1 g2 c,
  kk1 x = true -> loc_ok t1 -> kapply_op (km1 (key x)) DL x = (kop', out) ->
  prog_star (prouted (A ++ x :: B) t1 sR t2 km1 km2 g1 g2, c) (prouted (A ++ B) t1 sR t2 (kupdate km1 (key x) kop') km2 (out ++ g1) g2, c).
Proof.
  intros A x B t1 sR t2 km1 km2 kop' out g1 g2 c Hx Ht Hk. unfold prouted.
  destruct (feed_split x [] [] km1 [] [] km2) as (F1 & F2 & F3 & F4).
  eapply PS_step; [eapply lift_split; [apply split_op_L, Ht | rewrite (part_of_K1 _ Hx); exact F1]|].
  eapply PS_step; [eapply lift_kop1; [apply (kop_step_L [] x [] (TLoc l) [] (TLoc l)); [left; reflexivity | exact Hk]
                                     | apply feed_kop1_merge]|].
  apply PS_refl.
Qed.

Lemma route_L_K2 : forall A x B t1 sR t2 km1 km2 kop' out g1 g2 c,
  kk1 x = false -> loc_ok t1 -> kapply_op (km2 (key x)) DL x = (kop', out) ->
  prog_star (prouted (A ++ x :: B) t1 sR t2 km1 km2 g1 g2, c) (prouted (A ++ B) t1 sR t2 km1 (kupdate km2 (key x) kop') g1 (out ++ g2), c).
Proof.
  intros A x B t1 sR t2 km1 km2 kop' out g1 g2 c Hx Ht Hk. unfold prouted.
  destruct (feed_split x [] [] km1 [] [] km2) as (F1 & F2 & F3 & F4).
  eapply PS_step; [eapply lift_split; [apply split_op_L, Ht | rewrite (part_of_K2 _ Hx); exact F3]|].
  eapply PS_step; [eapply lift_kop2; [apply (kop_step_L [] x [] (TLoc l) [] (TLoc l)); [left; reflexivity | exact Hk]
                                     | apply feed_kop2_merge]|].
  apply PS_refl.
Qed.

Lemma route_R_K1 : forall sL t1 A x B t2 km1 km2 kop' out g1 g2 c,
  kk1 x = true -> loc_ok t2 -> kapply_op (km1 (key x)) DR x = (kop', out) ->
  prog_star (prouted sL t1 (A ++ x :: B) t2 km1 km2 g1 g2, c) (prouted sL t1 (A ++ B) t2 (kupdate km1 (key x) kop') km2 (out ++ g1) g2, c).
Proof.
  intros sL t1 A x B t2 km1 km2 kop' out g1 g2 c Hx Ht Hk. unfold prouted.
  destruct (feed_split x [] [] km1 [] [] km2) as (F1 & F2 & F3 & F4).
  eapply PS_step; [eapply lift_split; [apply split_op_R, Ht | rewrite (part_of_K1 _ Hx); exact F2]|].
  eapply PS_step; [eapply lift_kop1; [apply (kop_step_R [] x [] [] (TLoc l) (TLoc l)); [left; reflexivity | exact Hk]
                                     | apply feed_kop1_merge]|].
  apply PS_refl.
Qed.

Lemma route_R_K2 : forall sL t1 A x B t2 km1 km2 kop' out g1 g2 c,
  kk1 x = false -> loc_ok t2 -> kapply_op (km2 (key x)) DR x = (kop', out) ->
  prog_star (prouted sL t1 (A ++ x :: B) t2 km1 km2 g1 g2, c) (prouted sL t1 (A ++ B) t2 km1 (kupdate km2 (key x) kop') g1 (out ++ g2), c).
Proof.
  intros sL t1 A x B t2 km1 km2 kop' out g1 g2 c Hx Ht Hk. unfold prouted.
  destruct (feed_split x [] [] km1 [] [] km2) as (F1 & F2 & F3 & F4).
  eapply PS_step; [eapply lift_split; [apply split_op_R, Ht | rewrite (part_of_K2 _ Hx); exact F4]|].
  eapply PS_step; [eapply lift_kop2; [apply (kop_step_R [] x [] [] (TLoc l) (TLoc l)); [left; reflexivity | exact Hk]
                                     | apply feed_kop2_merge]|].
  apply PS_refl.
Qed.

(* The merge with inputs x (from the partition named by into_K1) and other. *)
Definition merge_with (into_K1 : bool) (x other : Buffer) : Expr :=
  if into_K1 then merge_st x other else merge_st other x.

(* Deliver rest (last value first, so rest ends up in order in front of the
   output context). *)
Lemma drain : forall s rest pre sp k1 k2 other bc tc, loc_ok tc ->
  prog_star (pipe sp k1 k2 (merge_with s (pre ++ rest) other), CBuf bc tc Unordered)
            (pipe sp k1 k2 (merge_with s pre other), CBuf (rest ++ bc) tc Unordered).
Proof.
  intros s. induction rest as [|r rest IH]; intros pre sp k1 k2 other bc tc Htc.
  - rewrite app_nil_r. apply PS_refl.
  - replace (pre ++ r :: rest) with ((pre ++ [r]) ++ rest) by (rewrite <- app_assoc; reflexivity).
    eapply prog_star_trans; [apply IH; exact Htc|].
    assert (Hs : step (merge_with s (pre ++ [r]) other) (merge_with s pre other) (CBuf [r] (TLoc l) Unordered)).
    { destruct s; simpl;
        [pose proof (merge_op_K1 pre r [] other) as H | pose proof (merge_op_K2 pre r [] other) as H];
        rewrite app_nil_r in H; exact H. }
    apply prog_star_one. eapply lift_merge; [exact Hs | rewrite cc_at_l; [reflexivity | exact Htc]].
Qed.

(* Partition kmaps keep agreeing after the kop and one partition kop both
   process v. *)
Ltac agree_close :=
  first [ apply agree_upd_in; assumption
        | apply agree_upd_out; [assumption | unfold kk1, kk2 in *; destruct (in_K1 (key _)); congruence] ].

Ltac fwd_close :=
  do 7 eexists; refine (conj eq_refl (conj eq_refl (conj _ (conj _ (conj _ (conj _ _))))));
  [ assumption | assumption | eexists; eexists; split; [reflexivity | assumption]
  | agree_close | agree_close ].

Lemma part_fwd_step : forall e e' c es cs,
  part_fwd e e' c -> prog_step (e, c) (es, cs) ->
  exists e's, prog_star (e', c) (e's, cs) /\ part_fwd es e's cs.
Proof.
  intros e e' c es cs (B1 & t1 & B2 & t2 & km & km1 & km2 & -> & -> & Ht1 & Ht2 & (bc & tc & -> & Htc) & Hag1 & Hag2)
         Hstep.
  apply prog_step_inv in Hstep. destruct Hstep as [em [Hs Hcc]].
  apply kop_step_inv in Hs.
  destruct Hs as [ (A & v & B & kop' & out & -> & Hk & -> & ->) | (A & v & B & kop' & out & -> & Hk & -> & ->) ];
    rewrite cc_at_l in Hcc by exact Htc; injection Hcc as <-; unfold pclean;
    destruct (kk1 v) eqn:Hkk;
    [ rewrite <- (Hag1 (key v) Hkk) in Hk | rewrite <- (Hag2 (key v) Hkk) in Hk
    | rewrite <- (Hag1 (key v) Hkk) in Hk | rewrite <- (Hag2 (key v) Hkk) in Hk ];
    eexists.
  - split.
    + eapply prog_star_trans; [apply (route_L_K1 A v B t1 B2 t2 km1 km2 kop' out [] [] _ Hkk Ht1 Hk)|].
      rewrite app_nil_r. apply (drain true out []); exact Htc.
    + fwd_close.
  - split.
    + eapply prog_star_trans; [apply (route_L_K2 A v B t1 B2 t2 km1 km2 kop' out [] [] _ Hkk Ht1 Hk)|].
      rewrite app_nil_r. apply (drain false out []); exact Htc.
    + fwd_close.
  - split.
    + eapply prog_star_trans; [apply (route_R_K1 B1 t1 A v B t2 km1 km2 kop' out [] [] _ Hkk Ht2 Hk)|].
      rewrite app_nil_r. apply (drain true out []); exact Htc.
    + fwd_close.
  - split.
    + eapply prog_star_trans; [apply (route_R_K2 B1 t1 A v B t2 km1 km2 kop' out [] [] _ Hkk Ht2 Hk)|].
      rewrite app_nil_r. apply (drain false out []); exact Htc.
    + fwd_close.
Qed.

Lemma part_fwd_feed : forall e e' c f ep,
  part_fwd e e' c -> feed f e = Some ep ->
  exists ep', feed f e' = Some ep' /\ part_fwd ep ep' c.
Proof.
  intros e e' c f ep (B1 & t1 & B2 & t2 & km & km1 & km2 & -> & -> & Ht1 & Ht2 & Hc & Hag1 & Hag2) Hf.
  destruct (feed_pair_inv _ _ _ _ _ _ _ Hf) as [-> | (f1 & f2 & x1 & x2 & -> & Hx1 & Hx2)].
  - simpl in Hf. inversion Hf; subst ep.
    eexists. split; [apply feed_pipe_CEps|].
    exists B1, t1, B2, t2, km, km1, km2. auto 10.
  - rewrite (feed_kconf_pair _ _ _ _ _ _ _ _ _ Hx1 Hx2) in Hf. inversion Hf; subst ep.
    eexists. split; [unfold pclean; apply (feed_pipe_pair _ _ x1 x2); assumption|].
    exists (x1 ++ B1), t1, (x2 ++ B2), t2, km, km1, km2. auto 10.
Qed.

Lemma part_fwd_ctx : forall e e' c π b b' t o c',
  part_fwd e e' c -> chan_get c π = Some (CBuf b t o) ->
  chan_set c π (CBuf b' t o) = Some c' -> part_fwd e e' c'.
Proof.
  intros e e' c π b b' t o c' (B1 & t1 & B2 & t2 & km & km1 & km2 & -> & -> & Ht1 & Ht2 & (bc & tc & -> & Htc) & Hag1 & Hag2)
         Hg Hs.
  destruct (chan_get_buf _ _ _ _ _ _ _ Hg) as (-> & _ & -> & ->).
  simpl in Hs. inversion Hs; subst c'.
  exists B1, t1, B2, t2, km, km1, km2. repeat split; auto. exists b', tc. auto.
Qed.

Lemma part_fwd_done : forall e e' c, part_fwd e e' c -> done e -> done e'.
Proof.
  intros e e' c (B1 & t1 & B2 & t2 & km & km1 & km2 & -> & -> & _) Hd.
  apply done_op_inv in Hd. simpl in Hd. destruct Hd as [-> ->].
  unfold pclean, pipe. repeat (apply F_Seq || apply F_Par).
  all: apply done_op; simpl; repeat split.
Qed.

(* ============================================================================
   9. Reverse simulation: kop processes a value exactly when a partition kop
      does; every other pipeline step is matched by no kop step.
   ========================================================================== *)

(* Closing tactic for a pipeline step that kop matches with no step. *)
Ltac rclose H1 H2 H3 H4 H5 :=
  do 18 eexists;
  refine (conj eq_refl (conj eq_refl (conj eq_refl (conj eq_refl
          (conj _ (conj _ (conj _ (conj _ (conj _ (conj _ (conj _ (conj _ (conj _ _)))))))))))));
  [ assumption | assumption | assumption
  | close_perm H1 | close_perm H2 | close_perm H3 | close_all_in H4 | close_all_in H5
  | assumption | assumption ].

(* The kop processes v, which it holds somewhere in an input. *)
Lemma kop_catch_up_L : forall B1 t1 B2 t2 km v kop' out R bL tc,
  loc_ok t1 -> loc_ok tc -> kapply_op (km (key v)) DL v = (kop', out) ->
  Permutation B1 (v :: R) ->
  exists B1', Permutation B1' R /\
    prog_star (kconf B1 t1 B2 t2 km, CBuf bL tc Unordered)
              (kconf B1' t1 B2 t2 (kupdate km (key v) kop'), CBuf (out ++ bL) tc Unordered).
Proof.
  intros B1 t1 B2 t2 km v kop' out R bL tc Ht Htc Hk Hp.
  destruct (perm_pick _ _ _ Hp) as (A & B & -> & HAB).
  exists (A ++ B). split; [exact HAB|].
  apply prog_star_one. eapply P_Step; [apply kop_step_L; eassumption | apply cc_at_l; eassumption].
Qed.

Lemma kop_catch_up_R : forall B1 t1 B2 t2 km v kop' out R bL tc,
  loc_ok t2 -> loc_ok tc -> kapply_op (km (key v)) DR v = (kop', out) ->
  Permutation B2 (v :: R) ->
  exists B2', Permutation B2' R /\
    prog_star (kconf B1 t1 B2 t2 km, CBuf bL tc Unordered)
              (kconf B1 t1 B2' t2 (kupdate km (key v) kop'), CBuf (out ++ bL) tc Unordered).
Proof.
  intros B1 t1 B2 t2 km v kop' out R bL tc Ht Htc Hk Hp.
  destruct (perm_pick _ _ _ Hp) as (A & B & -> & HAB).
  exists (A ++ B). split; [exact HAB|].
  apply prog_star_one. eapply P_Step; [apply kop_step_R; eassumption | apply cc_at_l; eassumption].
Qed.

(* v is in a concatenation that contains A ++ v :: B. *)
Ltac in_solve := first [ apply in_elt | apply in_or_app; first [ left; in_solve | right; in_solve ] ].

(* Closing tactic for a pipeline step where a partition kop processed v and
   kop caught up: HB is the perm of the kop input it read from. *)
Ltac kclose v HB H1 H2 H3 H4 H5 :=
  do 18 eexists;
  refine (conj eq_refl (conj eq_refl (conj eq_refl (conj eq_refl
          (conj _ (conj _ (conj _ (conj _ (conj _ (conj _ (conj _ (conj _ (conj _ _)))))))))))));
  [ assumption | assumption | assumption
  | first [close_perm HB | close_perm H1] | first [close_perm HB | close_perm H2]
  | eapply Permutation_trans; [apply Permutation_app_head; exact H3 | psolve]
  | first [eapply (all_in_drop _ _ v); [exact H4 | psolve] | close_all_in H4]
  | first [eapply (all_in_drop _ _ v); [exact H5 | psolve] | close_all_in H5]
  | agree_close | agree_close ].

Lemma part_rev_step : forall e e' cL cR e's cR',
  part_rev e e' cL cR -> prog_step (e', cR) (e's, cR') ->
  exists es cL', prog_star (e, cL) (es, cL') /\ part_rev es e's cL' cR'.
Proof.
  intros e e' cL cR e's cR'
    (B1 & t1 & B2 & t2 & km & split_L & split_R &
     kop1_L & kop1_R & km1 & kop2_L & kop2_R & km2 & merge_K1 & merge_K2 & out_L & out_R & tc &
     -> & -> & -> & -> & Ht1 & Ht2 & Htc & H1 & H2 & H3 & H4 & H5 & Hag1 & Hag2) Hstep.
  apply pipe_step_inv in Hstep.
  destruct Hstep as [ (s' & cs & ks' & Hs & Hf & -> & ->)
                    | [ (k1' & ck & g' & Hs & Hf & -> & ->)
                    | [ (k2' & ck & g' & Hs & Hf & -> & ->)
                      | (g' & cg & Hs & Hcc & ->) ] ] ].
  - (* split routes v to its partition: kop does nothing *)
    apply split_step_inv in Hs.
    destruct Hs as [ (A & v & B & -> & -> & ->) | (A & v & B & -> & -> & ->) ];
      destruct (kk1 v) eqn:Hkk;
      [ rewrite (part_of_K1 _ Hkk) in Hf | rewrite (part_of_K2 _ Hkk) in Hf
      | rewrite (part_of_K1 _ Hkk) in Hf | rewrite (part_of_K2 _ Hkk) in Hf ];
      destruct (feed_split v kop1_L kop1_R km1 kop2_L kop2_R km2) as (F1 & F2 & F3 & F4);
      [rewrite F1 in Hf | rewrite F3 in Hf | rewrite F2 in Hf | rewrite F4 in Hf]; injection Hf as <-;
      exists (kconf B1 t1 B2 t2 km), (CBuf out_L tc Unordered); (split; [apply PS_refl|]);
      rclose H1 H2 H3 H4 H5.
  - (* partition K1's kop processes v: so does kop *)
    apply kop_step_inv in Hs.
    destruct Hs as [ (A & v & B & kop' & out & -> & Hk & -> & ->) | (A & v & B & kop' & out & -> & Hk & -> & ->) ];
      rewrite feed_kop1_merge in Hf; injection Hf as <-;
      assert (Hkk : kk1 v = true) by (apply H4; in_solve);
      rewrite (Hag1 (key v) Hkk) in Hk.
    + assert (Hp : Permutation B1 (v :: (split_L ++ (A ++ B) ++ kop2_L))) by close_perm H1.
      destruct (kop_catch_up_L B1 t1 B2 t2 km v kop' out _ out_L tc Ht1 Htc Hk Hp) as (B1' & HB & Hss).
      eexists; eexists; split; [exact Hss | kclose v HB H1 H2 H3 H4 H5].
    + assert (Hp : Permutation B2 (v :: (split_R ++ (A ++ B) ++ kop2_R))) by close_perm H2.
      destruct (kop_catch_up_R B1 t1 B2 t2 km v kop' out _ out_L tc Ht2 Htc Hk Hp) as (B2' & HB & Hss).
      eexists; eexists; split; [exact Hss | kclose v HB H1 H2 H3 H4 H5].
  - (* partition K2's kop processes v: so does kop *)
    apply kop_step_inv in Hs.
    destruct Hs as [ (A & v & B & kop' & out & -> & Hk & -> & ->) | (A & v & B & kop' & out & -> & Hk & -> & ->) ];
      rewrite feed_kop2_merge in Hf; injection Hf as <-;
      assert (Hkk : kk1 v = false) by (apply kk2_kk1_t, H5; in_solve);
      rewrite (Hag2 (key v) Hkk) in Hk.
    + assert (Hp : Permutation B1 (v :: (split_L ++ kop1_L ++ (A ++ B)))) by close_perm H1.
      destruct (kop_catch_up_L B1 t1 B2 t2 km v kop' out _ out_L tc Ht1 Htc Hk Hp) as (B1' & HB & Hss).
      eexists; eexists; split; [exact Hss | kclose v HB H1 H2 H3 H4 H5].
    + assert (Hp : Permutation B2 (v :: (split_R ++ kop1_R ++ (A ++ B)))) by close_perm H2.
      destruct (kop_catch_up_R B1 t1 B2 t2 km v kop' out _ out_L tc Ht2 Htc Hk Hp) as (B2' & HB & Hss).
      eexists; eexists; split; [exact Hss | kclose v HB H1 H2 H3 H4 H5].
  - (* merge delivers v: kop delivered it already *)
    apply merge_step_inv in Hs.
    destruct Hs as [ (A & v & B & -> & -> & ->) | (A & v & B & -> & -> & ->) ];
      rewrite cc_at_l in Hcc by exact Htc; injection Hcc as <-;
      exists (kconf B1 t1 B2 t2 km), (CBuf out_L tc Unordered); (split; [apply PS_refl|]);
      rclose H1 H2 H3 H4 H5.
Qed.

Lemma part_rev_feed : forall e e' cL cR f ep',
  part_rev e e' cL cR -> feed f e' = Some ep' ->
  exists ep, feed f e = Some ep /\ part_rev ep ep' cL cR.
Proof.
  intros e e' cL cR f ep'
    (B1 & t1 & B2 & t2 & km & split_L & split_R &
     kop1_L & kop1_R & km1 & kop2_L & kop2_R & km2 & merge_K1 & merge_K2 & out_L & out_R & tc &
     -> & -> & -> & -> & Ht1 & Ht2 & Htc & H1 & H2 & H3 & H4 & H5 & Hag1 & Hag2) Hf.
  destruct (feed_pipe_inv _ _ _ _ _ _ _ _ _ Hf) as [-> | (f1 & f2 & x1 & x2 & -> & Hx1 & Hx2)].
  - rewrite feed_pipe_CEps in Hf. inversion Hf; subst ep'.
    exists (kconf B1 t1 B2 t2 km). split; [reflexivity|].
    do 18 eexists. repeat split; eauto.
  - rewrite (feed_pipe_pair _ _ _ _ _ _ _ _ _ _ _ Hx1 Hx2) in Hf. inversion Hf; subst ep'.
    exists (kconf (x1 ++ B1) t1 (x2 ++ B2) t2 km). split; [apply feed_kconf_pair; assumption|].
    do 18 eexists.
    refine (conj eq_refl (conj eq_refl (conj eq_refl (conj eq_refl
            (conj Ht1 (conj Ht2 (conj Htc (conj _ (conj _ (conj H3 (conj H4 (conj H5 (conj Hag1 Hag2)))))))))))));
      [ eapply Permutation_trans; [apply Permutation_app_head; exact H1 | psolve]
      | eapply Permutation_trans; [apply Permutation_app_head; exact H2 | psolve] ].
Qed.

Lemma part_rev_consume : forall e e' cL cR π v t o cR',
  part_rev e e' cL cR -> chan_remove cR π v t o cR' ->
  exists cL', chan_remove cL π v t o cL' /\ part_rev e e' cL' cR'.
Proof.
  intros e e' cL cR π v t o cR'
    (B1 & t1 & B2 & t2 & km & split_L & split_R &
     kop1_L & kop1_R & km1 & kop2_L & kop2_R & km2 & merge_K1 & merge_K2 & out_L & out_R & tc &
     -> & -> & -> & -> & Ht1 & Ht2 & Htc & H1 & H2 & H3 & H4 & H5 & Hag1 & Hag2)
    (A & B & Hg & Ho & Hs).
  destruct (chan_get_buf _ _ _ _ _ _ _ Hg) as (-> & HoR & -> & ->).
  simpl in Hs. inversion Hs; subst cR'.
  assert (Hp : Permutation out_L (v :: (merge_K1 ++ merge_K2 ++ A ++ B)))
    by (eapply Permutation_trans; [exact H3 | rewrite <- HoR; psolve]).
  destruct (perm_pick _ _ _ Hp) as (A' & B' & -> & HAB).
  exists (CBuf (A' ++ B') tc Unordered). split.
  - exists A', B'. split; [reflexivity|]. split; [unord | reflexivity].
  - do 18 eexists.
    refine (conj eq_refl (conj eq_refl (conj eq_refl (conj eq_refl
            (conj Ht1 (conj Ht2 (conj Htc (conj H1 (conj H2 (conj _ (conj H4 (conj H5 (conj Hag1 Hag2)))))))))))));
      close_perm HAB.
Qed.

Lemma part_rev_done : forall e e' cL cR, part_rev e e' cL cR -> done e' -> done e /\ chan_equiv cL cR.
Proof.
  intros e e' cL cR
    (B1 & t1 & B2 & t2 & km & split_L & split_R &
     kop1_L & kop1_R & km1 & kop2_L & kop2_R & km2 & merge_K1 & merge_K2 & out_L & out_R & tc &
     -> & -> & -> & -> & Ht1 & Ht2 & Htc & H1 & H2 & H3 & H4 & H5 & Hag1 & Hag2) Hd.
  unfold pipe in Hd.
  inversion Hd as [| x1 x2 Hd12 Hdg |]; subst.
  inversion Hd12 as [| x1 x2 Hds Hdk |]; subst.
  inversion Hdk as [x1 x2 Hdk1 Hdk2 | |]; subst.
  apply done_op_inv in Hds, Hdk1, Hdk2, Hdg. simpl in *.
  destruct Hds as [-> ->]. destruct Hdk1 as [-> ->]. destruct Hdk2 as [-> ->]. destruct Hdg as [-> ->].
  simpl in H1, H2, H3.
  apply Permutation_sym, Permutation_nil in H1. apply Permutation_sym, Permutation_nil in H2. subst.
  split.
  - apply done_op. simpl. auto.
  - constructor. exact H3.
Qed.

(* ============================================================================
   10. The Partitioning theorem.

       Replacing a kop whose input and output channels are Unordered by the
       partitioned pipeline, anywhere in a program (transform (rw_ext partition)),
       preserves the program's final outputs up to chan_equiv.
   ========================================================================== *)

Theorem Partitioning :
  forall e e' c,
    transform (rw_ext partition) e e' c ->
    wt_out e c ->
    forall c_final,
      (fin_output e c c_final -> exists c', fin_output e' c c' /\ chan_equiv c_final c') /\
      (fin_output e' c c_final -> exists c', fin_output e c c' /\ chan_equiv c' c_final).
Proof.
  apply (ext_fin_soundness partition part_fwd part_rev partition_fwd partition_rev).
  - exact (conj part_fwd_step (conj part_fwd_feed (conj part_fwd_ctx part_fwd_done))).
  - refine (conj _ (conj _ (conj part_rev_consume part_rev_done))).
    + intros e e' cL cR e's cR' H _ Hs. exact (part_rev_step _ _ _ _ _ _ H Hs).
    + intros e e' cL cR f ep' τ ci H _ _ _ Hf. exact (part_rev_feed _ _ _ _ _ _ H Hf).
Qed.

(* ============================================================================
   11. Partitioning preserves typing.

       If the kop is well typed in its context, so is the partitioned
       pipeline: its inputs are the kop's, its internal buffers are local and
       Unordered, and its output is produced at l like the kop's.
   ========================================================================== *)

Lemma loc_ok_sub : forall t, loc_ok t -> sub_leaf t (TLoc l).
Proof. intros t [-> | ->]; constructor. Qed.

Lemma partition_inp : forall e e' c ci τ,
  partition e e' c -> inp_chan e = Some ci -> chan_sub_type ci τ ->
  exists ci', inp_chan e' = Some ci' /\ chan_sub_type ci' τ.
Proof.
  intros e e' c ci τ (B1 & t1 & B2 & t2 & km & -> & -> & _) Hinp Hsub.
  exists ci. split; [exact Hinp | exact Hsub].
Qed.

(* The pipeline's internal buffers are local and Unordered, so they accept
   anything an operator at l produces. *)
Ltac local_sub :=
  repeat (simpl; constructor); try apply SL_Refl; try (let H := fresh in intro H; discriminate H).

Lemma partition_wt : forall e e' c, partition e e' c -> wt_out e c -> wt_out e' c.
Proof.
  intros e e' c (B1 & t1 & B2 & t2 & km & -> & -> & Ht1 & Ht2 & (bc & tc & -> & Htc)) [τ [Hwt Hco]].
  inversion Hwt as [| | cx ox lx κ1 κ2 Hsig Hcsub Hfifo]; subst.
  (* the kop's signature is forced by its input and output channels *)
  destruct κ1 as [|[|] [|]]; inversion Hcsub as [|? ? ? ? Ha Hb]; subst;
    try (inversion Ha; fail); try (inversion Hb; fail).
  destruct κ2 as [|? ?]; [|inversion Hco].
  (* it reads its Unordered inputs in any order *)
  assert (Hnf : ~ reads_fifo (o_kop km)) by (intro H; specialize (Hfifo H); discriminate Hfifo).
  assert (Hnf_s : ~ reads_fifo o_split) by (unfold reads_fifo; rewrite mode_split; discriminate).
  assert (Hnf_g : ~ reads_fifo o_merge) by (unfold reads_fifo; rewrite mode_merge; discriminate).
  eexists. split.
  - unfold pkop_of, pipe.
    eapply W_Seq_t; [ eapply W_Seq_t; [ | reflexivity | | apply W_Par_t ] | reflexivity | | ].
    + eapply W_Op_t; [exact sig_split | | intro H; contradiction].
      simpl. constructor; constructor; first [apply loc_ok_sub; assumption | discriminate].
    + local_sub.
    + eapply W_Op_t; [exact Hsig | local_sub | intro H; contradiction].
    + eapply W_Op_t; [exact Hsig | local_sub | intro H; contradiction].
    + local_sub.
    + eapply W_Op_t; [exact sig_merge | local_sub | intro H; contradiction].
  - simpl. constructor; [apply loc_ok_sub; exact Htc | discriminate].
Qed.

Theorem Partitioning_typing :
  forall e e' c, wt_out e c -> transform (rw_ext partition) e e' c -> wt_out e' c.
Proof.
  exact (Trans_Type_preservation (rw_ext partition)
           (typing_conds_ext partition (conj partition_inp partition_wt))).
Qed.
